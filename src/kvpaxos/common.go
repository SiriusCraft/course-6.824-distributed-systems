package kvpaxos

import "time"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

const PingInterval = time.Millisecond * 100

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me string
	Uid string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Me string
	Uid string
}

type GetReply struct {
	Err   Err
	Value string
}
