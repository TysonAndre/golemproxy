package memcache

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	MIN_WORKER_COUNT = 3
	// At most 100 requests can be waiting inside of a chan to be unqueued
	MAX_BACKLOG_SIZE = 100

	// There can be at most 100 requests in flight per worker. Not tuned.
	MAX_BACKLOG_PER_WORKER = 100

	// Needed in case of spurious errors, has to be finite in case memcache cluster is down.
	MAX_RETRY_COUNT = 3
)

// constant
var connectionEstablishError = errors.New("Unable to establish connection")
var noAvailableWorkersError = errors.New("No available workers")

// interface for testability
type ConnectionFactory interface {
	getConn() (*conn, error)
}

var _ ConnectionFactory = &PipeliningClient{}

type WorkerManager struct {
	maxWorkers         int
	createdWorkerCount int
	workChan           chan *workRequest
	connFactory        ConnectionFactory
}

type workRequest struct {
	// Serialization of the non-empty command to send to memcache
	// (e.g. to send a memcached Get request asynchronously)
	DataToWrite []byte
	// A callback which synchronously reads a non-zero number of bytes from r
	// (e.g. to process the result of a memcached Get request made earlier by the worker.)
	ResponseCB func(r *BufferedReader) error
	// TODO: RetryCB instead?
	// // remaining number of times this will retry (0 = no retries)

	// RemainingRetryCount int
	// Channel on which to send error or success, then close
	errChan chan<- error
}

type workFinalizeRequest struct {
	// A callback which synchronously reads a non-zero number of bytes from r
	// (e.g. to process the result of a memcached Get request made earlier by the worker.)
	ResponseCB func(r *BufferedReader) error
	// // remaining number of times this will retry if the request wasn't sent (0 = no retries)
	//RemainingRetryCount int

	// Channel on which to send error or success, then close
	errChan chan<- error
	// The reader corresponding to the writer this request was written on.
	// This is needed to gracefully handle old requests when reconnecting.
	reader *BufferedReader
}

func InitWorkerManager(manager *WorkerManager, maxWorkers int, connFactory ConnectionFactory) {
	// FIXME support multiple workers
	if maxWorkers < 1 {
		maxWorkers = 1
	}
	// XXX the maxWorkers setting is ignored, in practice this isn't useful
	manager.createdWorkerCount = 0
	manager.workChan = make(chan *workRequest, MAX_BACKLOG_SIZE)
	manager.connFactory = connFactory
	for i := 0; i < maxWorkers; i++ {
		go workerForConn(manager.workChan, manager.connFactory)
	}
	// Workers will be started lazily.
}

func (manager *WorkerManager) Finalize() {
	close(manager.workChan)
	manager.workChan = nil
}

type workerConnAndProcessor struct {
	conn                      *conn
	responseProcessingChannel chan<- workFinalizeRequest
	m                         sync.Mutex
}

func (wc *workerConnAndProcessor) Close() {
	// We take a pointer to workerConnAndProcessor because we modify the fields by value (e.g. wc.conn)
	wc.m.Lock()
	defer wc.m.Unlock()
	if wc.conn != nil {
		wc.conn.nc.Close()
		wc.conn = nil
		// We're done. Tell the response processor for wc.conn that there are no further commands to process.
		close(wc.responseProcessingChannel)
		wc.responseProcessingChannel = nil
	}
}

func (wc *workerConnAndProcessor) WriteOrClose(bytes []byte) error {
	// We take a pointer to workerConnAndProcessor because we modify the fields by value (e.g. wc.conn)
	for {
		if wc.conn.ShouldClose {
			wc.Close()
			return errors.New("Reader failed; closing writer")
		}
		// DebugLog("started write")
		n, err := wc.conn.writer.Write(bytes)
		// DebugLog("finished write")
		if err != nil {
			wc.Close()
			return err
		}
		if n < len(bytes) {
			if n == 0 {
				wc.Close()
				return errors.New("Could not write data")
			}
			bytes = bytes[n:]
			continue
		}
		return nil
	}
}

func NewWorkerConnAndProcessor(cf ConnectionFactory) (workerConnAndProcessor, error) {
	conn, err := cf.getConn()
	if err != nil {
		return workerConnAndProcessor{}, err
	}
	return workerConnAndProcessor{
		conn:                      conn,
		responseProcessingChannel: createResponseProcessorForConnection(),
	}, nil
}

// createResponseProcessorForConnection returns a channel that will process the responses for requests.
// They are inserted and processed asynchronously, but **in the same order** the requests were sent to memcache.
// If you open a new connection, you have to open a new response processor, and close the channel for the previous one.
func createResponseProcessorForConnection() chan<- workFinalizeRequest {
	tasksWithPendingResponsesChan := make(chan workFinalizeRequest, MAX_BACKLOG_PER_WORKER)

	// This goroutine processes the responses, asynchronously
	go func() {
		// Process tasks until the channel is closed.
		for task := range tasksWithPendingResponsesChan {
			// DebugLog("Reading a response from corresponding task.reader")
			err := task.ResponseCB(task.reader)
			// DebugLog("Read a response")
			if err == nil {
				close(task.errChan)
				continue
			}
			task.errChan <- err
			close(task.errChan)
			// In order to retry, this would need to send into a **different** channel, in order to make a request.
			/*
				}
				responseWorker.RemainingRetryCount--
				// Under heavy load, don't retry. This is a non-blocking attempt to re-insert into the channel, which may have filled up.
				// TODO: Better mechanism for this?
				// TODO: Insert into a different worker's channel?
				// TODO: The caller might close the channel
				select {
				case tasksWithPendingResponsesChan <- responseWorker:
					break
				default:
				}
			*/
		}
	}()
	return tasksWithPendingResponsesChan
}

var progStart = time.Now()

func DebugLog(message string) {
	fmt.Printf("%02.6f: %s\n", time.Since(progStart).Seconds(), message)
}

func workerForConn(workChan <-chan *workRequest, cf ConnectionFactory) {
	var connAndProcessor workerConnAndProcessor
	// nullBufReader := bufio.NewReader(nullReader{})

	processRequests := func(requests []*workRequest, dataToWrite []byte) {
		err := connAndProcessor.WriteOrClose(dataToWrite)
		if err != nil {
			for _, request := range requests {
				request.errChan <- err
				close(request.errChan)
			}
			return
		}
		// Writing the batch of commands was successful.
		for _, request := range requests {
			connAndProcessor.responseProcessingChannel <- workFinalizeRequest{
				errChan:    request.errChan,
				ResponseCB: request.ResponseCB,
				reader:     connAndProcessor.conn.reader,
			}
		}
	}

	buf := []byte{}

	nonBlockingReadRequest := func() *workRequest {
		select {
		case additionalRequest := <-workChan:
			return additionalRequest
		default:
			return nil
		}
	}

	rejectPendingRequests := func(request *workRequest, err error) {
		request.errChan <- err
		close(request.errChan)
		for {
			select {
			case otherRequest, ok := <-workChan:
				// fmt.Fprintf(os.Stderr, "Closing another request\n")
				if !ok {
					return
				}
				otherRequest.errChan <- err
				close(otherRequest.errChan)
			default:
				// fmt.Fprintf(os.Stderr, "No more requests seen\n")
				return
			}
		}
	}

	for {
		request, ok := <-workChan
		// DebugLog("Received request")
		if !ok {
			return
		}
		if connAndProcessor.conn == nil {
			// DebugLog("Have a null conn, creating new conn")
			var err error
			connAndProcessor, err = NewWorkerConnAndProcessor(cf)
			fmt.Fprintf(os.Stderr, "Established a connection to a memcache server, err=%v\n", err)
			if err != nil {
				rejectPendingRequests(request, err)
				continue
			}
		}
		// Writes to errChan should be non-blocking, callers should allocate with capacity 1.
		//workRequest.errChan <- noAvailableWorkersError
		//close(workRequest.errChan)

		// Write (and implicitly flush) the string to memcache (TODO: Only buffer reads, don't buffer writes)
		connAndProcessor.conn.extendDeadline()
		// Non-blocking read for additional request data
		additionalRequest := nonBlockingReadRequest()
		// DebugLog("Finished read request")

		if additionalRequest != nil {
			// DebugLog("Picked up additional requests")
			// There are 2 or more commands to buffer.

			// Clear and re-use the buffer.
			// This buffer contains the data for the requests, serialized in the order they were sent.
			buf = buf[:0]
			buf = append(buf, request.DataToWrite...)
			buf = append(buf, additionalRequest.DataToWrite...)
			requests := []*workRequest{request, additionalRequest}

			for len(requests) <= 10 && len(buf) <= 1000000 {
				additionalRequest = nonBlockingReadRequest()
				if additionalRequest == nil {
					break
				}
				requests = append(requests, additionalRequest)
				buf = append(buf, additionalRequest.DataToWrite...)
			}
			// fmt.Printf("Batch size = %d\n", len(requests))
			processRequests(requests, buf)
			// DebugLog("Done processing additional requests")
			continue
		}
		// fmt.Printf("Batch size = 1\n")
		// There's a single command
		err := connAndProcessor.WriteOrClose([]byte(request.DataToWrite))
		if err != nil {
			rejectPendingRequests(request, err)
			continue
		}

		// Asynchronously process the response for the request we just sent to memcache.
		// By processing this way, we can have MAX_BACKLOG_PER_WORKER requests in flight at the same time per socket.
		connAndProcessor.responseProcessingChannel <- workFinalizeRequest{
			errChan:    request.errChan,
			ResponseCB: request.ResponseCB,
			reader:     connAndProcessor.conn.reader,
		}
		// DebugLog("Finished processing single request")
	}
}

// sendRequestToWorker will send a request to a worker, or stop if no workers are available.
func (c *WorkerManager) sendRequestToWorker(dataToWrite []byte, readFn func(*BufferedReader) error) <-chan error {
	errChan := make(chan error, 1)
	// TODO: This will retry 2 times if we receive the connection error before sending the command.
	// However, if workChan fills up, this won't retry.
	request := &workRequest{
		DataToWrite: dataToWrite,
		ResponseCB:  readFn,
		// RemainingRetryCount: 2,
		errChan: errChan,
	}
	/**
	 * 1. Send a request to a member of the pool of workers, or start a new worker if that fails.
	 * 2. Each worker is a goroutine, and has a channel of requests for a given twemproxy socket group.
	 * 3. Goroutines accept from a shared channel? Multiple channels?
	 * 4. Due to the read timeouts, I think that they'll stop automatically.
	 */
	// XXX is this any better?
	select {
	case c.workChan <- request:
		break
	default:
		errChan <- noAvailableWorkersError
		close(errChan)
	}
	return errChan
}
