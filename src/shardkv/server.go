package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


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
    ReconfigurationOp = 4
    SyncOp = 5
)

type Op struct {
	// Your definitions here.
	Type       int
	GID        int64
    Key         string
    Value       string
    Client      string
    Seq         int
    Config      shardmaster.Config
    SyncData    SyncReply
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	gid 	   int64 // my replica group ID

	// Your definitions here.
	config 	      shardmaster.Config
	data          map[string]string
	seen 	   	  map[string]int
	replyOfErr 	  map[string]Err
	replyOfValue  map[string]string
	seq 	      int
}

func (kv *ShardKV) checkSeen(op Op) bool {
    seq, client := op.Seq, op.Client
	lastSeq, seenExists := kv.seen[client]
    if (seenExists && lastSeq == seq) {
    	return true
    }
    return false
}

func isSameOp(op1 Op, op2 Op) bool {
    if op1.Client == op2.Client && op1.Seq == op2.Seq {
        return true
    }
    return false
}

func (kv *ShardKV) applyOp(op Op) {
   	key, gid := op.Key, op.GID
   	client, seq := op.Client, op.Seq
   	switch op.Type {
   	case GetOp:
   		// check if wrong group
		if (gid != kv.gid) {
			kv.replyOfErr[client] = ErrWrongGroup
			return
		}

		// get the value
		value, exists := kv.data[key]

		// set the reply
		kv.seen[client] = seq
   		if (exists) {
   			kv.replyOfErr[client] = OK
   		} else {
   			kv.replyOfErr[client] = ErrNoKey
   		}
   		kv.replyOfValue[client] = value

   	case PutOp:
   		// check if wrong group
        if (gid != kv.gid) {
			kv.replyOfErr[client] = ErrWrongGroup
			return
		}

		// put the value
   		value, _ := kv.data[key]
   		kv.data[key] = op.Value

   		// set the reply
   		kv.seen[client] = seq
   		kv.replyOfErr[client] = OK
   		kv.replyOfValue[client] = value

   	case AppendOp:
   		// check if wrong group
		if (gid != kv.gid) {
			kv.replyOfErr[client] = ErrWrongGroup
			return
		}

		// append the value
   		value, exists := kv.data[key]
   		if (exists) {
   			kv.data[key] += op.Value
   		}

   		// set the reply
		kv.seen[client] = seq
   		if (exists) {
   			kv.replyOfErr[client] = OK
   		} else {
   			kv.replyOfErr[client] = ErrNoKey
   		}
   		kv.replyOfValue[client] = value

  	case ReconfigurationOp:
  		if kv.config.Num >= op.Config.Num {
            kv.replyOfErr[client] = OK
            return
        }

        // sync data
        for key := range op.SyncData.Data {
            kv.data[key] = op.SyncData.Data[key]
        }

        // sync clients' states
        for client := range op.SyncData.Seen {
            seq, exists := kv.seen[client]
            if !exists || seq < op.SyncData.Seen[client] {
                kv.seen[client] = kv.seen[client]
                kv.replyOfErr[client] = op.SyncData.ReplyOfErr[client]
                kv.replyOfValue[client] = op.SyncData.ReplyOfValue[client]
            }
        }

        // update config
        kv.config = op.Config
   	}
}

func (kv *ShardKV) waitUntilAgreement(seq int, expectedOp Op) bool {
    to := 10 * time.Millisecond
    for {
    	status, instance := kv.px.Status(seq)
        if status == paxos.Decided {
            op := instance.(Op)
            kv.seen[op.Client] = op.Seq

            kv.applyOp(op) 

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

func (kv *ShardKV) startOp(op Op) (Err, string) {
    for {
        kv.seq = kv.seq + 1
        // check last seen for at-most-once semantic
        if (kv.checkSeen(op)) {
        	break
        }
        
        kv.px.Start(kv.seq, op)
        if kv.waitUntilAgreement(kv.seq, op) {
            break
        }
    }

    return kv.replyOfErr[op.Client], kv.replyOfValue[op.Client]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
    defer kv.mu.Unlock()
    
    // construct GetOp
    seq, client := args.Seq, args.Me
    key, gid := args.Key, args.GID
    op := Op{Type: GetOp, GID: gid, Key: key, Client: client, Seq: seq}
    reply.Err, reply.Value = kv.startOp(op)

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
    defer kv.mu.Unlock()
    
    // construct GetOp
    seq, client, opType := args.Seq, args.Me, args.Op
    key, value, gid := args.Key, args.Value, args.GID
    var op Op
    if opType == "Put" {
    	op = Op{Type: PutOp, GID: gid, Key: key, Value: value, Client: client, Seq: seq}
	} else {
		op = Op{Type: AppendOp, GID: gid, Key: key, Value: value, Client: client, Seq: seq}
	}
    reply.Err, _ = kv.startOp(op)	

	return nil
}

func (kv *ShardKV) Sync(args *SyncArgs, reply *SyncReply) error {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    if kv.config.Num < args.Config.Num {
        reply.Err = ErrNotReady
        return nil
    }

    // construct SyncOp
    shard := args.Shard
    op := Op{Type: SyncOp}
    reply.Err, _ = kv.startOp(op)

    reply.Data = make(map[string]string)
    reply.Seen = make(map[string]int)
    reply.ReplyOfErr = make(map[string]Err)
    reply.ReplyOfValue = make(map[string]string)

    for key := range kv.data {
        if key2shard(key) == shard {
            reply.Data[key] = kv.data[key]
        }
    }
    for client := range kv.seen {
        reply.Seen[client] = kv.seen[client]
        reply.ReplyOfErr[client] = kv.replyOfErr[client]
        reply.ReplyOfValue[client] = kv.replyOfValue[client]
    }

    return nil
}

func (reply *SyncReply) merge(otherReply SyncReply) {
    // merge data
    for key := range otherReply.Data {
        reply.Data[key] = otherReply.Data[key]
    }

    // merge clients' states
    for client := range otherReply.Seen {
        seq, exists := reply.Seen[client]
        if !exists || seq < otherReply.Seen[client] {
            reply.Seen[client] = otherReply.Seen[client]
            reply.ReplyOfErr[client] = otherReply.ReplyOfErr[client]
            reply.ReplyOfValue[client] = otherReply.ReplyOfValue[client]
        }
    }
}

func (kv *ShardKV) reConfigure(newConfig shardmaster.Config) bool {
    // get all synced data
    oldConfig := &kv.config
    syncData := SyncReply{OK, map[string]string{}, map[string]int{},
        map[string]Err{}, map[string]string{}}
    for i := 0; i < shardmaster.NShards; i++ {
        if newConfig.Shards[i] == kv.gid && oldConfig.Shards[i] != kv.gid {
            args := &SyncArgs{}
            args.Shard = i
            args.Config = *oldConfig
            synced := false
            var reply SyncReply
            for _, srv := range oldConfig.Groups[oldConfig.Shards[i]] {
                ok := call(srv, "ShardKV.", args, &reply)
                if ok && reply.Err == OK {
                    synced = true
                    break
                }
            }
            if (!synced) {
                return false
            }

            syncData.merge(reply)
        }
    }

    // construct ReconfigurationOp
    op := Op{Type: ReconfigurationOp, Config: newConfig, SyncData: syncData}
    kv.startOp(op)

    return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    // get latest config
    latestConfig := kv.sm.Query(-1)
    // re-configure one by one in order
    for i := kv.config.Num + 1; i <= latestConfig.Num; i++ {
        newConfig := kv.sm.Query(i)
        if (!kv.reConfigure(newConfig)) {
            return
        }
    }
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
    kv.config = shardmaster.Config{Num:-1}
	kv.data = make(map[string]string)
	kv.seen = make(map[string]int)
	kv.replyOfErr = make(map[string]Err)
	kv.replyOfValue = make(map[string]string)
	kv.seq = -1

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
