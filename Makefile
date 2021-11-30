# This file is deprecated since v1.0
# Go parameters
# Set TABSIZE as 8 to avoid stupid mistake like  *** missing separator
PWD := $(shell pwd)
GOPATH := $(shell go env GOPATH)

GOARCH := $(shell go env GOARCH)
GOOS := $(shell go env GOOS)

PROJECT_NAME=hdr
BINARY_NAME=hdr
BINARY_UNIX=$(BINARY_NAME)_unix

all:    build 
build:
	go build -o $(BINARY_NAME) -v main.go erasure-encode.go
test: 
	go test -v ./...
clean:
	go clean
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_UNIX)
run:
	go build -o $(BINARY_NAMEs) -v main.go erasure-encode.go
	./$(BINARY_NAME)
deps:
        # go get github.com/markbates/goth

install:
	@echo "Installing minio binary to '$(GOPATH)/bin/$(PROJECT_NAME)'"
	@mkdir -p $(GOPATH)/bin && cp -f $(PWD)/$(BINARY_NAME) $(GOPATH)/bin/$(PROJECT_NAME)
	@echo "Installation successful."

# Cross compilation
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BINARY_UNIX) -v
docker-build:
	# docker run --rm -it -v "$(GOPATH)":/go -w /go/src/bitbucket.org/rsohlich/makepost golang:latest go build -o "$(BINARY_UNIX)" -v

