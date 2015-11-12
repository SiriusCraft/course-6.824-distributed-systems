package main

//
// toy RPC library
//

import "io"
import "fmt"
import "sync"
import "encoding/binary"

type ToyClient struct {
	mu      sync.Mutex
	conn    io.ReadWriteCloser
	xid     int64
	pending map[int64]chan int32
}

func MakeToyClient(conn io.ReadWriteCloser) *ToyClient {
	tc := &ToyClient{}
	tc.conn = conn
	tc.pending = map[int64]chan int32{}
	tc.xid = 1
	go tc.Listener()
	return tc
}

func (tc *ToyClient) WriteRequest(xid int64, procNum int32, arg int32) {
	binary.Write(tc.conn, binary.LittleEndian, xid)
	binary.Write(tc.conn, binary.LittleEndian, procNum)
	binary.Write(tc.conn, binary.LittleEndian, arg)
}

func (tc *ToyClient) ReadReply() (int64, int32) {
	var xid int64
	var arg int32
	binary.Read(tc.conn, binary.LittleEndian, &xid)
	binary.Read(tc.conn, binary.LittleEndian, &arg)
	return xid, arg
}

func (tc *ToyClient) Call(procNum int32, arg int32) int32 {
	done := make(chan int32)

	tc.mu.Lock()
	xid := tc.xid
	tc.xid++
	tc.pending[xid] = done
	tc.WriteRequest(xid, procNum, arg)
	tc.mu.Unlock()

	reply := <-done

	tc.mu.Lock()
	delete(tc.pending, xid)
	tc.mu.Unlock()

	return reply
}

func (tc *ToyClient) Listener() {
	for {
		xid, reply := tc.ReadReply()
		tc.mu.Lock()
		ch, ok := tc.pending[xid]
		tc.mu.Unlock()
		if ok {
			ch <- reply
		}
	}
}

type ToyServer struct {
	mu       sync.Mutex
	conn     io.ReadWriteCloser
	handlers map[int32]func(int32) int32
}

func MakeToyServer(conn io.ReadWriteCloser) *ToyServer {
	ts := &ToyServer{}
	ts.conn = conn
	ts.handlers = map[int32](func(int32) int32){}
	go ts.Dispatcher()
	return ts
}
