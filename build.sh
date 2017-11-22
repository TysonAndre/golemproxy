#!/usr/bin/env bash
set -xeu
go tool vet -all .
go build ./...
go test ./... -race
