package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"
import "strconv"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

const (
	OK 			 = "OK"
	REJECT 		 = "REJECT"
)

const (
	INITIAL_NUMBER = "0"
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	instances  map[int]*PaxosInstance
	dones map[int]int
}

type PaxosArgs struct {
	Seq int
	Number string
	Value interface{}
	Done int
	Me int
}

type PaxosReply struct {
	Result string
	Number string
	Value interface{}
}

type PaxosInstance struct {
	number string
	acceptedNumber string
	value interface{}
	decided bool
}

func (px *Paxos) MakePaxosInstance(seq int, v interface{}) {
	px.instances[seq] = &PaxosInstance{
		number: INITIAL_NUMBER,
		acceptedNumber: INITIAL_NUMBER, 
		value: v}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


// Paxos Algorithm - Proposer
func (px *Paxos) generateProposalNumber() string {
	duration := time.Now().Sub(time.Date(1994, time.January, 30, 8, 0, 0, 0, time.UTC))
	return strconv.FormatInt(duration.Nanoseconds(), 10) + "-" + strconv.Itoa(px.me)
}

func (px *Paxos) sendPrepare(seq int, n string, v interface{}) (bool, string, interface{}) {
	number := INITIAL_NUMBER
	value := v
	okCnt := 0
	args := PaxosArgs{Seq: seq, Number: n}
	reply := PaxosReply{Result: REJECT}

	for i, acceptor := range px.peers {
		if i == px.me {
			px.ProcessPrepare(&args, &reply)
		} else {
			call(acceptor, "Paxos.ProcessPrepare", &args, &reply)
		}
		if reply.Result == OK {
			if reply.Number > number {
				number = reply.Number
				value = reply.Value
			}
			okCnt++
		}
	}

	ok := okCnt > len(px.peers) / 2
	return ok, number, value
}

func (px *Paxos) sendAccept(seq int, n string, v interface{}) bool {
	okCnt := 0
	args := PaxosArgs{Seq: seq, Number: n, Value: v}
	reply := PaxosReply{Result: REJECT}
	for i, acceptor := range px.peers {
		if i == px.me {
			px.ProcessAccept(&args, &reply)
		} else {
			call(acceptor, "Paxos.ProcessAccept", &args, &reply)
		}
		if reply.Result == OK {
			okCnt++
		}
	}

	ok := okCnt > len(px.peers) / 2
	return ok
}

func (px *Paxos) sendDecision(seq int, n string, v interface{}) {
	px.instances[seq].decided = true
	for i, acceptor := range px.peers {
		go func(i int, acceptor string) {
			args := PaxosArgs{Seq: seq, Number: n, Value: v, Done: px.dones[px.me], Me: px.me}
			reply := PaxosReply{}
			if i != px.me {
				call(acceptor, "Paxos.ProcessDecision", &args, &reply)
			}
		}(i, acceptor)
	}
}

func (px *Paxos) propose(seq int, v interface{}) {
	for {
		n := px.generateProposalNumber()
		ok, number, value := px.sendPrepare(seq, n, v)
		if ok {
			ok = px.sendAccept(seq, number, value)
		}
		if ok {
			px.sendDecision(seq, number, value)
			break
		}
	}
}

// Paxos Algorithm - Acceptor
func (px *Paxos) ProcessPrepare(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	seq := args.Seq
	number := args.Number
	reply.Result = REJECT

	_, exist := px.instances[seq]
	if !exist {
		px.MakePaxosInstance(seq, nil)
		reply.Result = OK
	} else {
		if (px.instances[seq].number < number) {
			reply.Result = OK
		}
	}
	
	if reply.Result == OK {
		reply.Number = px.instances[seq].acceptedNumber
		reply.Value = px.instances[seq].value
		px.instances[seq].number = number
	}

	return nil
}

func (px *Paxos) ProcessAccept(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	seq := args.Seq
	number := args.Number
	value := args.Value
	reply.Result = REJECT

	_, exist := px.instances[seq]
	if !exist {
		px.MakePaxosInstance(seq, nil)
	}
	
	if number >= px.instances[seq].number {
		px.instances[seq].acceptedNumber = number
		px.instances[seq].value = value
		reply.Result = OK
	}

	return nil
}

func (px *Paxos) ProcessDecision(args *PaxosArgs, reply *PaxosReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	seq := args.Seq
	number := args.Number
	value := args.Value

	_, exist := px.instances[seq]
	if !exist {
		px.MakePaxosInstance(seq, nil)
	}
	
	px.instances[seq].acceptedNumber = number
	px.instances[seq].value = value
	px.instances[seq].decided = true

	px.dones[args.Me] = args.Done

	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go func() {
		if (seq < px.Min()) {
			return
		}
		px.propose(seq, v)
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.dones[px.me] < seq {
		px.dones[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return 0
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	minSeq = px.dones[px.me]
	for _, seq := range px.dones {
		if minSeq > seq {
			minSeq = seq
		}
	}

	for seq, _ := range px.instances {
		if seq <= minSeq {
			delete(px.instances, seq)
		}
	}

	return minSeq + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	return Pending, nil
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	px.instances = map[int]*PaxosInstance{}
	px.dones = map[int]int{}
	for i := range px.peers {
		px.dones[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}