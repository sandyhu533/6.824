package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type GetPutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append" or Get
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UniqueRequestId string
}

type GetPutAppendReply struct {
	Err Err
	Success bool
	Value string
}

//type GetArgs struct {
//	Key string
//	// You'll have to add definitions here.
//	UniqueRequestId string
//}
//
//type GetReply struct {
//	Err   Err
//	Success bool
//	Value string
//}
