package graceful

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	// The tests will run a test server on this port.
	port               = 9654
	concurrentRequestN = 8
	killTime           = 500 * time.Millisecond
	timeoutTime        = 1000 * time.Millisecond
	waitTime           = 100 * time.Millisecond
)

func runQuery(t *testing.T, expected int, shouldErr bool, wg *sync.WaitGroup, once *sync.Once) {
	defer wg.Done()
	client := http.Client{}
	r, err := client.Get(fmt.Sprintf("http://localhost:%d", port))
	if shouldErr && err == nil {
		once.Do(func() {
			t.Error("Expected an error but none was encountered.")
		})
	} else if shouldErr && err != nil {
		if checkErr(t, err, once) {
			return
		}
	}
	if r != nil && r.StatusCode != expected {
		once.Do(func() {
			t.Errorf("Incorrect status code on response. Expected %d. Got %d", expected, r.StatusCode)
		})
	} else if r == nil {
		once.Do(func() {
			t.Error("No response when a response was expected.")
		})
	}
}

func checkErr(t *testing.T, err error, once *sync.Once) bool {
	if err.(*url.Error).Err == io.EOF {
		return true
	}
	var errno syscall.Errno
	switch oe := err.(*url.Error).Err.(type) {
	case *net.OpError:
		switch e := oe.Err.(type) {
		case syscall.Errno:
			errno = e
		case *os.SyscallError:
			errno = e.Err.(syscall.Errno)
		}
		if errno == syscall.ECONNREFUSED {
			return true
		} else if err != nil {
			once.Do(func() {
				t.Error("Error on Get:", err)
			})
		}
	default:
		if strings.Contains(err.Error(), "transport closed before response was received") {
			return true
		}
		if strings.Contains(err.Error(), "server closed connection") {
			return true
		}
		fmt.Printf("unknown err: %s, %#v\n", err, err)
	}
	return false
}

func createListener(sleep time.Duration) (*http.Server, net.Listener, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
		time.Sleep(sleep)
		rw.WriteHeader(http.StatusOK)
	})

	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	return server, l, err
}

func launchTestQueries(t *testing.T, wg *sync.WaitGroup, c chan os.Signal) {
	defer wg.Done()
	var once sync.Once

	for i := 0; i < concurrentRequestN; i++ {
		wg.Add(1)
		go runQuery(t, http.StatusOK, false, wg, &once)
	}

	time.Sleep(waitTime)
	c <- os.Interrupt
	time.Sleep(waitTime)

	for i := 0; i < concurrentRequestN; i++ {
		wg.Add(1)
		go runQuery(t, 0, true, wg, &once)
	}
}

func TestGracefulRun(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()

	c := make(chan os.Signal, 1)
	server, l, err := createListener(killTime / 2)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv := &Server{Timeout: killTime, Server: server, interrupt: c}
		srv.Serve(l)
	}()

	wg.Add(1)
	go launchTestQueries(t, &wg, c)
}

func TestGracefulRunLimitKeepAliveListener(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()

	c := make(chan os.Signal, 1)
	server, l, err := createListener(killTime / 2)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv := &Server{
			Timeout:      killTime,
			ListenLimit:  concurrentRequestN,
			TCPKeepAlive: 1 * time.Second,
			Server:       server,
			interrupt:    c,
		}
		srv.Serve(l)
	}()

	wg.Add(1)
	go launchTestQueries(t, &wg, c)
}

func TestGracefulRunTimesOut(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()

	c := make(chan os.Signal, 1)
	server, l, err := createListener(killTime * 10)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv := &Server{Timeout: killTime, Server: server, interrupt: c}
		srv.Serve(l)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		var once sync.Once

		for i := 0; i < concurrentRequestN; i++ {
			wg.Add(1)
			go runQuery(t, 0, true, &wg, &once)
		}

		time.Sleep(waitTime)
		c <- os.Interrupt
		time.Sleep(waitTime)

		for i := 0; i < concurrentRequestN; i++ {
			wg.Add(1)
			go runQuery(t, 0, true, &wg, &once)
		}
	}()
}

func TestGracefulRunDoesntTimeOut(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()

	c := make(chan os.Signal, 1)
	server, l, err := createListener(killTime * 2)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv := &Server{Timeout: 0, Server: server, interrupt: c}
		srv.Serve(l)
	}()

	wg.Add(1)
	go launchTestQueries(t, &wg, c)
}

func TestGracefulRunDoesntTimeOutAfterConnectionCreated(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()

	c := make(chan os.Signal, 1)
	server, l, err := createListener(killTime)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv := &Server{Timeout: 0, Server: server, interrupt: c}
		srv.Serve(l)
	}()
	time.Sleep(waitTime)

	// Make a sample first request. The connection will be left idle.
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d", port))
	if err != nil {
		panic(fmt.Sprintf("first request failed: %v", err))
	}
	resp.Body.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()

		// With idle connections improperly handled, the server doesn't wait for this
		// to complete and the request fails. It should be allowed to complete successfully.
		_, err := http.Get(fmt.Sprintf("http://localhost:%d", port))
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
	}()

	// Ensure the request goes out
	time.Sleep(waitTime)
	c <- os.Interrupt
	wg.Wait()
}

func TestGracefulRunNoRequests(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()

	c := make(chan os.Signal, 1)
	server, l, err := createListener(killTime * 2)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv := &Server{Timeout: 0, Server: server, interrupt: c}
		srv.Serve(l)
	}()

	c <- os.Interrupt
}

func TestGracefulForwardsConnState(t *testing.T) {
	var stateLock sync.Mutex
	states := make(map[http.ConnState]int)
	connState := func(conn net.Conn, state http.ConnState) {
		stateLock.Lock()
		states[state]++
		stateLock.Unlock()
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	expected := map[http.ConnState]int{
		http.StateNew:    concurrentRequestN,
		http.StateActive: concurrentRequestN,
		http.StateClosed: concurrentRequestN,
	}

	c := make(chan os.Signal, 1)
	server, l, err := createListener(killTime / 2)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv := &Server{
			ConnState: connState,
			Timeout:   killTime,
			Server:    server,
			interrupt: c,
		}
		srv.Serve(l)
	}()

	wg.Add(1)
	go launchTestQueries(t, &wg, c)
	wg.Wait()

	// The forwarded callback for the final state of a connection can still be
	// running after Serve returns, so allow it a little time to arrive.
	var actual map[http.ConnState]int

	deadline := time.Now().Add(timeoutTime)

	for {
		stateLock.Lock()
		actual = maps.Clone(states)
		stateLock.Unlock()

		if reflect.DeepEqual(actual, expected) || time.Now().After(deadline) {
			break
		}

		time.Sleep(10 * time.Millisecond)
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Incorrect connection state tracking.\n  actual: %v\nexpected: %v\n", actual, expected)
	}
}

func TestGracefulExplicitStop(t *testing.T) {
	server, l, err := createListener(1 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	srv := &Server{Timeout: killTime, Server: server}

	go func() {
		go srv.Serve(l)
		time.Sleep(waitTime)
		srv.Stop(killTime)
	}()

	// block on the stopChan until the server has shut down
	select {
	case <-srv.StopChan():
	case <-time.After(timeoutTime):
		t.Fatal("Timed out while waiting for explicit stop to complete")
	}
}

func TestGracefulExplicitStopOverride(t *testing.T) {
	server, l, err := createListener(1 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	srv := &Server{Timeout: killTime, Server: server}

	go func() {
		go srv.Serve(l)
		time.Sleep(waitTime)
		srv.Stop(killTime / 2)
	}()

	// block on the stopChan until the server has shut down
	select {
	case <-srv.StopChan():
	case <-time.After(killTime):
		t.Fatal("Timed out while waiting for explicit stop to complete")
	}
}

func TestBeforeShutdownAndShutdownInitiatedCallbacks(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()

	server, l, err := createListener(1 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	beforeShutdownCalled := make(chan struct{})
	cb1 := func() bool { close(beforeShutdownCalled); return true }
	shutdownInitiatedCalled := make(chan struct{})
	cb2 := func() { close(shutdownInitiatedCalled) }

	wg.Add(2)
	srv := &Server{Server: server, BeforeShutdown: cb1, ShutdownInitiated: cb2}
	go func() {
		defer wg.Done()
		srv.Serve(l)
	}()
	go func() {
		defer wg.Done()
		time.Sleep(waitTime)
		srv.Stop(killTime)
	}()

	beforeShutdown := false
	shutdownInitiated := false
	for i := 0; i < 2; i++ {
		select {
		case <-beforeShutdownCalled:
			beforeShutdownCalled = nil
			beforeShutdown = true
		case <-shutdownInitiatedCalled:
			shutdownInitiatedCalled = nil
			shutdownInitiated = true
		case <-time.After(killTime):
			t.Fatal("Timed out while waiting for ShutdownInitiated callback to be called")
		}
	}

	if !beforeShutdown {
		t.Fatal("beforeShutdown should be true")
	}
	if !shutdownInitiated {
		t.Fatal("shutdownInitiated should be true")
	}
}

func TestBeforeShutdownCanceled(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	server, l, err := createListener(1 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	beforeShutdownCalled := make(chan struct{})
	cb1 := func() bool { close(beforeShutdownCalled); return false }
	shutdownInitiatedCalled := make(chan struct{})
	cb2 := func() { close(shutdownInitiatedCalled) }

	srv := &Server{Server: server, BeforeShutdown: cb1, ShutdownInitiated: cb2}
	go func() {
		srv.Serve(l)
		wg.Done()
	}()
	go func() {
		time.Sleep(waitTime)
		srv.Stop(killTime)
	}()

	beforeShutdown := false
	shutdownInitiated := false
	timeouted := false

	for i := 0; i < 2; i++ {
		select {
		case <-beforeShutdownCalled:
			beforeShutdownCalled = nil
			beforeShutdown = true
		case <-shutdownInitiatedCalled:
			shutdownInitiatedCalled = nil
			shutdownInitiated = true
		case <-time.After(killTime):
			timeouted = true
		}
	}

	if !beforeShutdown {
		t.Fatal("beforeShutdown should be true")
	}
	if !timeouted {
		t.Fatal("timeouted should be true")
	}
	if shutdownInitiated {
		t.Fatal("shutdownInitiated shouldn't be true")
	}

	srv.BeforeShutdown = func() bool { return true }
	srv.Stop(killTime)

	wg.Wait()
}

func hijackingListener(srv *Server) (*http.Server, net.Listener, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
		conn, bufrw, err := rw.(http.Hijacker).Hijack()
		if err != nil {
			http.Error(rw, "webserver doesn't support hijacking", http.StatusInternalServerError)
			return
		}

		defer conn.Close()

		bufrw.WriteString("HTTP/1.1 200 OK\r\n\r\n")
		bufrw.Flush()
	})

	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	return server, l, err
}

func TestNotifyClosed(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()

	c := make(chan os.Signal, 1)
	srv := &Server{Timeout: killTime, interrupt: c}
	server, l, err := hijackingListener(srv)
	if err != nil {
		t.Fatal(err)
	}

	srv.Server = server

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.Serve(l)
	}()

	var once sync.Once
	for i := 0; i < concurrentRequestN; i++ {
		wg.Add(1)
		runQuery(t, http.StatusOK, false, &wg, &once)
	}

	srv.Stop(0)

	// block on the stopChan until the server has shut down
	select {
	case <-srv.StopChan():
	case <-time.After(timeoutTime):
		t.Fatal("Timed out while waiting for explicit stop to complete")
	}

	if len(srv.connections) > 0 {
		t.Fatal("hijacked connections should not be managed")
	}

}

func TestStopDeadlock(t *testing.T) {
	var wg sync.WaitGroup
	defer wg.Wait()

	c := make(chan struct{})
	server, l, err := createListener(1 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	srv := &Server{Server: server, NoSignalHandling: true}

	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(waitTime)
		srv.Serve(l)
	}()
	go func() {
		defer wg.Done()
		srv.Stop(0)
		close(c)
	}()

	select {
	case <-c:
		l.Close()
	case <-time.After(timeoutTime):
		t.Fatal("Timed out while waiting for explicit stop to complete")
	}
}

// Run with --race
func TestStopRace(t *testing.T) {
	server, l, err := createListener(1 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	srv := &Server{Timeout: killTime, Server: server}

	go func() {
		go srv.Serve(l)
		srv.Stop(killTime)
	}()
	srv.Stop(0)
	select {
	case <-srv.StopChan():
	case <-time.After(timeoutTime):
		t.Fatal("Timed out while waiting for explicit stop to complete")
	}
}

func TestInterruptLog(t *testing.T) {
	c := make(chan os.Signal, 1)

	server, l, err := createListener(killTime * 10)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	var tbuf bytes.Buffer
	logger := log.New(&buf, "", 0)
	expected := log.New(&tbuf, "", 0)

	srv := &Server{Timeout: killTime, Server: server, Logger: logger, interrupt: c}
	go func() { srv.Serve(l) }()

	stop := srv.StopChan()
	c <- os.Interrupt
	expected.Print("shutdown initiated")

	<-stop

	if buf.String() != tbuf.String() {
		t.Fatal("shutdown log incorrect - got '" + buf.String() + "'")
	}
}

func TestMultiInterrupts(t *testing.T) {
	c := make(chan os.Signal, 1)

	server, l, err := createListener(killTime * 10)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var bu bytes.Buffer
	buf := SyncBuffer{&wg, &bu}
	var tbuf bytes.Buffer
	logger := log.New(&buf, "", 0)
	expected := log.New(&tbuf, "", 0)

	srv := &Server{Timeout: killTime, Server: server, Logger: logger, interrupt: c}
	go func() { srv.Serve(l) }()

	stop := srv.StopChan()
	buf.Add(1 + 10) // Expecting 11 log calls
	c <- os.Interrupt
	expected.Printf("shutdown initiated")
	for i := 0; i < 10; i++ {
		c <- os.Interrupt
		expected.Printf("already shutting down")
	}

	<-stop

	wg.Wait()
	bb, bt := buf.Bytes(), tbuf.Bytes()
	for i, b := range bb {
		if b != bt[i] {
			t.Fatal(fmt.Sprintf("shutdown log incorrect - got '%s', expected '%s'", buf.String(), tbuf.String()))
		}
	}
}

func TestLogFunc(t *testing.T) {
	c := make(chan os.Signal, 1)

	server, l, err := createListener(killTime * 10)
	if err != nil {
		t.Fatal(err)
	}
	var called bool
	srv := &Server{Timeout: killTime, Server: server,
		LogFunc: func(format string, args ...interface{}) {
			called = true
		}, interrupt: c}
	stop := srv.StopChan()
	go func() { srv.Serve(l) }()
	c <- os.Interrupt
	<-stop

	if called != true {
		t.Fatal("Expected LogFunc to be called.")
	}
}

// SyncBuffer calls Done on the embedded wait group after each call to Write.
type SyncBuffer struct {
	*sync.WaitGroup
	*bytes.Buffer
}

func (buf *SyncBuffer) Write(b []byte) (int, error) {
	defer buf.Done()
	return buf.Buffer.Write(b)
}

// TestStopTimeoutWithActiveConnectionHasNoRace checks that a Stop whose
// timeout expires while a request is still in flight, and while idle
// connections are being closed, does not race with net/http's own connection
// state handling. Run with -race.
func TestStopTimeoutWithActiveConnectionHasNoRace(t *testing.T) {
	for range 10 {
		stopWithTimeoutDuringActiveRequest(t, 16)
	}
}

func stopWithTimeoutDuringActiveRequest(t *testing.T, idleConns int) {
	t.Helper()

	states := newConnStates()
	inHandler := make(chan struct{})
	release := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/block", func(rw http.ResponseWriter, _ *http.Request) {
		close(inHandler)
		<-release
		rw.WriteHeader(http.StatusOK)
	})

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := &Server{
		Server:           &http.Server{Handler: mux, ReadHeaderTimeout: timeoutTime},
		NoSignalHandling: true,
		ConnState:        states.record,
	}

	served := make(chan error, 1)

	go func() { served <- srv.Serve(l) }()

	url := "http://" + l.Addr().String()
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: idleConns}}

	defer client.CloseIdleConnections()

	openIdleConnections(t, client, url, idleConns)

	clientErr := make(chan error, 1)

	go func() {
		clientErr <- get(t, http.DefaultClient, url+"/block")
	}()

	select {
	case <-inHandler:
	case <-time.After(timeoutTime):
		t.Fatal("request never reached the handler")
	}

	// A tiny timeout means the kill path runs while net/http is still
	// updating the state of the idle connections that shutdown closes.
	srv.Stop(time.Nanosecond)

	select {
	case <-srv.StopChan():
	case <-time.After(timeoutTime):
		t.Fatal("timed out waiting for the server to stop")
	}

	select {
	case err = <-clientErr:
		if err == nil {
			t.Error("expected the in-flight request to be killed")
		}
	case <-time.After(timeoutTime):
		t.Fatal("in-flight connection was not closed when the timeout expired")
	}

	close(release)

	select {
	case <-served:
	case <-time.After(timeoutTime):
		t.Fatal("Serve did not return")
	}

	// Every connection was opened before Stop, so all must be reported closed,
	// including those closed by the kill path. (The client may reuse
	// connections, so there can be fewer than idleConns+1 of them.)
	opened, closed := states.waitAllClosed(timeoutTime)
	if opened < 2 || closed != opened {
		t.Fatalf("expected StateClosed forwarded for every connection; opened %d, closed %d", opened, closed)
	}
}

// connStates records which connections a user ConnState callback has seen
// opened and closed.
type connStates struct {
	mu     sync.Mutex
	opened map[net.Conn]bool
	closed map[net.Conn]bool
}

func newConnStates() *connStates {
	return &connStates{opened: make(map[net.Conn]bool), closed: make(map[net.Conn]bool)}
}

func (c *connStates) record(conn net.Conn, state http.ConnState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch state {
	case http.StateNew:
		c.opened[conn] = true
	case http.StateClosed:
		c.closed[conn] = true
	default:
	}
}

// waitAllClosed waits up to timeout for every opened connection to have been
// reported closed, returning the opened and closed counts.
func (c *connStates) waitAllClosed(timeout time.Duration) (int, int) {
	deadline := time.Now().Add(timeout)

	for {
		c.mu.Lock()
		opened, closed := len(c.opened), len(c.closed)
		c.mu.Unlock()

		if (opened > 0 && opened == closed) || time.Now().After(deadline) {
			return opened, closed
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// openIdleConnections makes n concurrent requests with client so that n
// keep-alive connections are left idle on the server.
func openIdleConnections(t *testing.T, client *http.Client, url string, n int) {
	t.Helper()

	var wg sync.WaitGroup

	errs := make(chan error, n)

	for range n {
		wg.Add(1)

		go func() {
			defer wg.Done()

			errs <- get(t, client, url)
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// get makes a GET request to url with client and reads the whole response.
func get(t *testing.T, client *http.Client, url string) error {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	_, err = io.Copy(io.Discard, resp.Body)

	return err
}
