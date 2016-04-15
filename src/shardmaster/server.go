package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	seq int // paxos seq
	configNum // current config num
}

const (
	JOIN 1
	LEAVE 2
	MOVE 3
	QUERY 4
)

type Op struct {
	// Your data here.
	Type int
	GID int
	Servers []string
	Shard int
	ConfigNum int
}

func (kv *KVPaxos) waitForAgreement(seq int, expectedOp Op) bool {
    to := 10 * time.Millisecond
    for {
        status, instance := kv.px.Status(seq)
        if status == paxos.Decided {
            op := instance.(Op)

            if op.Type == JOIN {
            } else if (op.Type == LEAVE) {
            } else if (op.Type == MOVE) {
           	}
            
            kv.px.Done(seq)
            if (isSameOp(op, expectedOp)) {
                return true
            } else {
                return false
            }
        }
        time.Sleep(to)
        if to < 10 * time.Second {
          to *= 2
        }
    }
    return false
}

func (sm *ShardMaster) startInstance(instance interface{}) {
	for {
		sm.seq = sm.seq + 1
		sm.px.Start(sm.seq, instance)
		if sm.waitForAgreement(sm.seq, instance) {
			break
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Lock()

	gid := args.GID
	servers := args.Servers

	instance := Op{Type: JOIN, GID: gid, Servers: servers}
	startInstance(instance)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Lock()

	gid := args.GID
	
	instance := Op{Type: LEAVE, GID: gid}
	startInstance(instance)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Lock()

	gid := args.GID
	shard := args.Shard
	
	instance := Op{Type: MOVE, GID: gid, Shard: shard}
	startInstance(instance)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Lock()

	confignum := args.Num
	
	instance := Op{Type: QUERY, ConfigNum: confignum}
	startInstance(instance)

	if confignum == -1 || confignum > sm.configNum {
		reply.Config = sm.configs[sm.configNum]
	} else {
		reply.Config = sm.configs[confignum]
	}

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l


	sm.seq = -1
	sm.configNum = 0

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
