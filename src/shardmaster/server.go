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
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	seq int // paxos seq
	configNum int// current config num
}

const (
	JOIN = 1
	LEAVE = 2
	MOVE = 3
	QUERY = 4
)

type Op struct {
	// Your data here.
	Type int
	GID int64
	Servers []string
	Shard int
	ConfigNum int
	UUID int64
}

func (sm* ShardMaster) getNewConfig() *Config {
	oldConfig := &sm.configs[sm.configNum]

	// copy old config
	var newConfig Config
	newConfig.Num = oldConfig.Num + 1
	newConfig.Groups = map[int64][]string{}
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	newConfig.Shards = [NShards]int64{}
	for shard, gid := range oldConfig.Shards {
		newConfig.Shards[shard] = gid
	}

	sm.configNum++
	sm.configs = append(sm.configs, newConfig)
	return &sm.configs[sm.configNum]
}

func getMinAndMaxGID(config *Config) (int64, int64) {
	minGID, minShardNum, maxGID, maxShardNum := int64(0), NShards + 1, int64(0), -1
	counts := map[int64]int{}

	for gid := range config.Groups {
		counts[gid] = 0
	}
	for _, gid := range config.Shards {
		if gid == 0 {
			return 0, 0
		}
		counts[gid]++
	}

	for gid, count := range counts {
		if minShardNum > count {
			minGID = gid
			minShardNum = count
		}
		if maxShardNum < count {
			maxGID = gid
			maxShardNum = count
		}
	}

	return minGID, maxGID
}

func getShardByGID(config *Config, gid int64) int {
	for shard, g := range config.Shards {
		if g == gid {
			return shard
		}
	}
	return -1
}

func (sm* ShardMaster) rebalance(gid int64, isLeave bool) {
	config := &sm.configs[sm.configNum]
	for i := 0; ; i++ {
		minGID, maxGID := getMinAndMaxGID(config)
		if isLeave {
			shard := getShardByGID(config, gid) 
			if shard == -1 {
				break
			}
			config.Shards[shard] = minGID
		} else {
			if i == NShards / len(config.Groups) {
				break
			}
			shard := getShardByGID(config, maxGID)
			config.Shards[shard] = gid
		}
	}
}

func (sm* ShardMaster) applyJoin(gid int64, servers []string) {
	config := sm.getNewConfig()
	_, exists := config.Groups[gid]
	if !exists {
		config.Groups[gid] = servers
		// rebalance
	}
}

func (sm* ShardMaster) applyLeave(gid int64) {
	config := sm.getNewConfig()
	_, exists := config.Groups[gid]
	if !exists {
		delete(config.Groups, gid)
		// rebalance
	}
}

func (sm* ShardMaster) applyMove(gid int64, shard int) {
	config := sm.getNewConfig()
	config.Shards[shard] = gid
}

func (sm* ShardMaster) waitForAgreement(seq int, expectedOp Op) bool {
    to := 10 * time.Millisecond
    for {
        status, instance := sm.px.Status(seq)
        if status == paxos.Decided {
            op := instance.(Op)
            gid, servers, shard := op.GID, op.Servers, op.Shard

            switch (op.Type) {
            case JOIN:
            	sm.applyJoin(gid, servers)
            case LEAVE:
            	sm.applyLeave(gid)
            case MOVE:
            	sm.applyMove(gid, shard)
           	}
            
            sm.px.Done(seq)
            if op.UUID == expectedOp.UUID {
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

func (sm *ShardMaster) startInstance(instance Op) {
	instance.UUID = nrand()
	for {
		sm.seq = sm.seq + 1
		sm.px.Start(sm.seq, instance)
		if sm.waitForAgreement(sm.seq, instance) {
			break
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	fmt.Println("Query")
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Lock()

	gid := args.GID
	servers := args.Servers

	instance := Op{Type: JOIN, GID: gid, Servers: servers}
	sm.startInstance(instance)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	fmt.Println("Query")
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Lock()

	gid := args.GID
	
	instance := Op{Type: LEAVE, GID: gid}
	sm.startInstance(instance)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	fmt.Println("Query")
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Lock()

	gid := args.GID
	shard := args.Shard
	
	instance := Op{Type: MOVE, GID: gid, Shard: shard}
	sm.startInstance(instance)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	fmt.Println("Query")
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Lock()

	confignum := args.Num
	
	instance := Op{Type: QUERY, ConfigNum: confignum}
	sm.startInstance(instance)

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
