#!/usr/bin/env bash
set -xeu
go vet -all .
go build ./...
go test ./... -race
