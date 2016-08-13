package diskv

import "shardmaster"
import "paxos"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq   int
	Me    string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq int
	Me string
}

type GetReply struct {
	Err   Err
	Value string
}

type SyncArgs struct {
	Shard  int
	Config shardmaster.Config
	Reboot bool
}

type SyncReply struct {
	Err 		  Err
	Data 		  map[string]string
	Seen 	   	  map[string]int
	ReplyOfErr 	  map[string]Err
	ReplyOfValue  map[string]string
	Instances     map[int]paxos.PaxosInstance
    Dones         map[int]int
}