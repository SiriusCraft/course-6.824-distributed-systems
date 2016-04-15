package kvpaxos

import "time"

const (
    OK       = "OK"
    ErrNoKey = "ErrNoKey"
)

const PingInterval = 1000 * time.Millisecond

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
    UUid string
}

type PutAppendReply struct {
    Err Err
}

type GetArgs struct {
    Key string
    // You'll have to add definitions here.
    Me string
    UUid string
}

type GetReply struct {
    Err   Err
    Value string
}
