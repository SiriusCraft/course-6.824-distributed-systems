package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GetOp = 1
	PutOp = 2
	AppendOp = 3
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type int
	Key string
	Value string
	Client string
	Uid string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	client     map[string]string
	reply 	   map[string]string
	seq		   int
	content	   map[string]string
}

func isSameOp(op1 Op, op2 Op) bool {
	if op1.Client == op2.Client && op1.Uid == op2.Uid {
		return true
	}
	return false
}

func (kv *KVPaxos) Wait(seq int, expectedOp Op) bool {
	to := 10 * time.Millisecond
	for {
		status, op := kv.px.Status(seq)
		if status == paxos.Decided {
    		kv.client[op.(Op).Client] = op.(Op).Uid
    		if (op.(Op).Type == PutOp) {
    			kv.content[op.(Op).Key] = op.(Op).Value
    		} else if (op.(Op).Type == AppendOp) {
    			kv.content[op.(Op).Key] += op.(Op).Value
    		} else {
    			kv.reply[op.(Op).Client] = kv.content[op.(Op).Key]
    		}
    		
      		if (isSameOp(op.(Op), expectedOp)) {
      			return true
      		} else {
      			return false
      		}
      		kv.px.Done(seq)
    	}
	    time.Sleep(to)
	    if to < 10 * time.Second {
	      to *= 2
	    }
	}
	return false
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	client, uid := args.Me, args.Uid
	if kv.client[client] == uid {
		reply.Value = kv.reply[client]
		fmt.Printf("uid = %s reply = %s\n", uid, reply.Value)
		return nil
	}
	paxosOp := Op {Type: GetOp, Key: key, Client: client, Uid: uid}
	for {
		kv.seq = kv.seq + 1
		kv.px.Start(kv.seq, paxosOp)
		if kv.Wait(kv.seq, paxosOp) {
			kv.reply[client] = kv.content[key]
			break
		}
	}

	reply.Value = kv.reply[client]
	fmt.Printf("uid = %s reply = %s\n", uid, reply.Value)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key, value, op := args.Key, args.Value, args.Op
	client, uid := args.Me, args.Uid
	if kv.client[client] == uid {
		return nil
	}
	var paxosOp Op
	if op == "PutOp" {
		paxosOp = Op{Type: PutOp, Key: key, Value: value, Client: client, Uid: uid}
	} else {
		paxosOp = Op{Type: AppendOp, Key: key, Value: value, Client: client, Uid: uid}
	}
	for {
		kv.seq = kv.seq + 1
		kv.px.Start(kv.seq, paxosOp)
		if kv.Wait(kv.seq, paxosOp) {
			break
		}
	}

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.client = make(map[string]string)
	kv.reply = make(map[string]string)
	kv.seq = -1
	kv.content = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
