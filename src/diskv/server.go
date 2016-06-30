package diskv

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
import "encoding/base32"
import "math/rand"
import "shardmaster"
import "io/ioutil"
import "strconv"


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
	Type        int
	Key         string
    Value       string
    Client      string
    Seq         int
    Config      shardmaster.Config
    SyncData    SyncReply
}


type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

	gid int64 // my replica group ID

	// Your definitions here.
	config 	      shardmaster.Config
	data          map[string]string
	seen 	   	  map[string]int
	replyOfErr 	  map[string]Err
	replyOfValue  map[string]string
	seq 	      int
}

//
// these are handy functions that might be useful
// for reading and writing key/value files, and
// for reading and writing entire shards.
// puts the key files for each shard in a separate
// directory.
//

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

// cannot use keys in file names directly, since
// they might contain troublesome characters like /.
// base32-encode the key to get a file name.
// base32 rather than base64 b/c Mac has case-insensitive
// file names.
func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// read the content of a key's file.
func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// replace the content of a key's file.
// uses rename() to make the replacement atomic with
// respect to crashes.
func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// return content of every key file in a given shard.
func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{}
	d := kv.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)
			}
			content, err := kv.fileGet(shard, key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}

// replace an entire shard directory.
func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range m {
		kv.filePut(shard, k, v)
	}
}

func (kv *DisKV) checkSeen(op Op) bool {
    seq, client := op.Seq, op.Client
	lastSeq, seenExists := kv.seen[client]
    if seenExists && lastSeq >= seq {
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

func (kv *DisKV) applyOp(op Op) {
   	key, client, seq := op.Key, op.Client, op.Seq
   	switch op.Type {
   	case GetOp:
   		// check if wrong group
        if (kv.config.Shards[key2shard(key)] != kv.gid) {
			kv.replyOfErr[client] = ErrWrongGroup
			return
		}
        // check last seen for at-most-once semantic
        if kv.checkSeen(op) {
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
        if (kv.config.Shards[key2shard(key)] != kv.gid) {
            kv.replyOfErr[client] = ErrWrongGroup
            return
        }
        // check last seen for at-most-once semantic
        if kv.checkSeen(op) {
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
        if (kv.config.Shards[key2shard(key)] != kv.gid) {
            kv.replyOfErr[client] = ErrWrongGroup
            return
        }
        // check last seen for at-most-once semantic
        if kv.checkSeen(op) {
            return
        }

		// append the value
   		value, _ := kv.data[key]
   		kv.data[key] += op.Value

   		// set the reply
		kv.seen[client] = seq
   		kv.replyOfErr[client] = OK
   		kv.replyOfValue[client] = value

  	case ReconfigurationOp:
  		if kv.config.Num >= op.Config.Num {
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
                kv.seen[client] = op.SyncData.Seen[client]
                kv.replyOfErr[client] = op.SyncData.ReplyOfErr[client]
                kv.replyOfValue[client] = op.SyncData.ReplyOfValue[client]
            }
        }

        // update config
        kv.config = op.Config
    }
}

func (kv *DisKV) waitUntilAgreement(seq int, expectedOp Op) bool {
    to := 10 * time.Millisecond
    for {
    	status, instance := kv.px.Status(seq)
        if status == paxos.Decided {
            op := instance.(Op)

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

func (kv *DisKV) startOp(op Op) (Err, string) {
    for {
        kv.seq = kv.seq + 1
        
        kv.px.Start(kv.seq, op)
        if kv.waitUntilAgreement(kv.seq, op) {
            break
        }
    }

    return kv.replyOfErr[op.Client], kv.replyOfValue[op.Client]
}

func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
    defer kv.mu.Unlock()
	
	// construct GetOp
    seq, client, key := args.Seq, args.Me, args.Key
    op := Op{Type: GetOp, Key: key, Client: client, Seq: seq}
    reply.Err, reply.Value = kv.startOp(op)

	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
    defer kv.mu.Unlock()

    // construct GetOp
    seq, client, opType := args.Seq, args.Me, args.Op
    key, value := args.Key, args.Value
    var op Op
    if opType == "Put" {
    	op = Op{Type: PutOp, Key: key, Value: value, Client: client, Seq: seq}
	} else {
		op = Op{Type: AppendOp, Key: key, Value: value, Client: client, Seq: seq}
	}
    reply.Err, _ = kv.startOp(op)
    
	return nil
}

func (kv *DisKV) Sync(args *SyncArgs, reply *SyncReply) error {
    if kv.config.Num < args.Config.Num {
        reply.Err = ErrNotReady
        return nil
    }

    kv.mu.Lock()
    defer kv.mu.Unlock()

    // construct SyncOp
    shard := args.Shard
    op := Op{Type: SyncOp}
    kv.startOp(op)

    reply.Err = OK
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

func (kv *DisKV) reConfigure(newConfig shardmaster.Config) bool {
    // get all synced data
    oldConfig := &kv.config
    syncData := SyncReply{OK, map[string]string{}, map[string]int{},
        map[string]Err{}, map[string]string{}}
    for i := 0; i < shardmaster.NShards; i++ {
        if newConfig.Shards[i] == kv.gid && oldConfig.Shards[i] != kv.gid && len(oldConfig.Groups[oldConfig.Shards[i]]) > 0 {
            args := &SyncArgs{}
            args.Shard = i
            args.Config = *oldConfig
            synced := false
            var reply SyncReply
            for _, srv := range oldConfig.Groups[oldConfig.Shards[i]] {
                ok := call(srv, "DisKV.Sync", args, &reply)
                if ok && reply.Err == OK {
                    synced = true
                    break
                }
            }
            if !synced {
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
func (kv *DisKV) tick() {
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
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
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
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, dir string, restart bool) *DisKV {

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir

	// Your initialization code here.
	// Don't call Join().
	kv.config = shardmaster.Config{Num:-1}
	kv.data = make(map[string]string)
	kv.seen = make(map[string]int)
	kv.replyOfErr = make(map[string]Err)
	kv.replyOfValue = make(map[string]string)
	kv.seq = -1

	// log.SetOutput(ioutil.Discard)

	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	// log.SetOutput(os.Stdout)



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
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
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
