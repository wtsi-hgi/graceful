graceful [![Go Reference](https://pkg.go.dev/badge/github.com/wtsi-hgi/graceful.svg)](https://pkg.go.dev/github.com/wtsi-hgi/graceful) [![tests](https://github.com/wtsi-hgi/graceful/actions/workflows/tests.yml/badge.svg)](https://github.com/wtsi-hgi/graceful/actions/workflows/tests.yml) [![golangci-lint](https://github.com/wtsi-hgi/graceful/actions/workflows/golangci-lint.yml/badge.svg)](https://github.com/wtsi-hgi/graceful/actions/workflows/golangci-lint.yml)
========

Graceful is a Go package enabling graceful shutdown of http.Handler servers.

This is a maintained fork of the archived
[tylerb/graceful](https://github.com/tylerb/graceful), published as the Go
module `github.com/wtsi-hgi/graceful`. It fixes a data race in which a `Stop`
timeout expiring with active connections wrote `http.Server.ConnState` while
`net/http` was reading it.

One behaviour changes as a result: a user `ConnState` callback now receives
`StateClosed` for connections closed when the `Stop` timeout expires. Before,
those notifications were dropped racily or their goroutines hung.

## Using Go 1.8?

If you are using Go 1.8, you may not need to use this library! Consider using `http.Server`'s built-in [Shutdown()](https://golang.org/pkg/net/http/#Server.Shutdown)
method for graceful shutdowns.

## Installation

To install, simply execute:

```
go get github.com/wtsi-hgi/graceful
```

## Usage

Using Graceful is easy. Simply create your http.Handler and pass it to the `Run` function:

```go
package main

import (
  "github.com/wtsi-hgi/graceful"
  "net/http"
  "fmt"
  "time"
)

func main() {
  mux := http.NewServeMux()
  mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
    fmt.Fprintf(w, "Welcome to the home page!")
  })

  graceful.Run(":3001",10*time.Second,mux)
}
```

Another example, using [Negroni](https://github.com/codegangsta/negroni), functions in much the same manner:

```go
package main

import (
  "github.com/codegangsta/negroni"
  "github.com/wtsi-hgi/graceful"
  "net/http"
  "fmt"
  "time"
)

func main() {
  mux := http.NewServeMux()
  mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
    fmt.Fprintf(w, "Welcome to the home page!")
  })

  n := negroni.Classic()
  n.UseHandler(mux)
  //n.Run(":3000")
  graceful.Run(":3001",10*time.Second,n)
}
```

In addition to Run there are the http.Server counterparts ListenAndServe, ListenAndServeTLS and Serve, which allow you to configure HTTPS, custom timeouts and error handling.
Graceful may also be used by instantiating its Server type directly, which embeds an http.Server:

```go
mux := // ...

srv := &graceful.Server{
  Timeout: 10 * time.Second,

  Server: &http.Server{
    Addr: ":1234",
    Handler: mux,
  },
}

srv.ListenAndServe()
```

This form allows you to set the ConnState callback, which works in the same way as in http.Server:

```go
mux := // ...

srv := &graceful.Server{
  Timeout: 10 * time.Second,

  ConnState: func(conn net.Conn, state http.ConnState) {
    // conn has a new state
  },

  Server: &http.Server{
    Addr: ":1234",
    Handler: mux,
  },
}

srv.ListenAndServe()
```

## Behaviour

When Graceful is sent a SIGINT or SIGTERM (possibly from ^C or a kill command), it:

1. Disables keepalive connections.
2. Closes the listening socket, allowing another process to listen on that port immediately.
3. Starts a timer of `timeout` duration to give active requests a chance to finish.
4. When timeout expires, closes all active connections.
5. Closes the `stopChan`, waking up any blocking goroutines.
6. Returns from the function, allowing the server to terminate.

## Notes

If the `timeout` argument to `Run` is 0, the server never times out, allowing all active requests to complete.

If you wish to stop the server in some way other than an OS signal, you may call the `Stop()` function.
This function stops the server, gracefully, using the new timeout value you provide. The `StopChan()` function
returns a channel on which you can block while waiting for the server to stop. This channel will be closed when
the server is stopped, allowing your execution to proceed. Multiple goroutines can block on this channel at the
same time and all will be signalled when stopping is complete.

### Important things to note when setting `timeout` to 0:

If you set the `timeout` to `0`, it waits for all connections to the server to disconnect before shutting down. 
This means that even though requests over a connection have finished, it is possible for the client to hold the
connection open and block the server from shutting down indefinitely.

This is especially evident when graceful is used to run HTTP/2 servers. Clients like Chrome and Firefox have been
observed to hold onto the open connection indefinitely over HTTP/2, preventing the server from shutting down. In 
addition, there is also the risk of malicious clients holding and keeping the connection alive.

It is understandable that sometimes, you might want to wait for the client indefinitely because they might be
uploading large files. In these type of cases, it is recommended that you set a reasonable timeout to kill the
connection, and have the client perform resumable uploads. For example, the client can divide the file into chunks
and reupload chunks that were in transit when the connection was terminated.

## Contributing

If you would like to contribute, please:

1. Create a GitHub issue regarding the contribution. Features and bugs should be discussed beforehand.
2. Fork the repository.
3. Create a pull request with your solution. This pull request should reference and close the issues (Fix #2).

All pull requests should:

1. Pass [gometalinter -t .](https://github.com/alecthomas/gometalinter) with no warnings.
2. Be `go fmt` formatted.
