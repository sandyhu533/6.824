package kvraft

import (
	"../labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	pointer *big.Int
	me string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.pointer = big.NewInt(0)
	ck.mu = sync.Mutex{}
	ck.me = time.Now().String()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
type Req struct {
	mu      sync.Mutex
	UniqueRequestId	string
	Success	bool
	Value string
}
func (ck *Clerk) Get(key string) string {
	return ck.GetPutAppend(key, "", OpGet)
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.GetPutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) GetPutAppend(key string, value string, op string) string{
	ck.mu.Lock()
	uniqueRequestId := ck.me + ck.pointer.String()
	ck.pointer = ck.pointer.Add(ck.pointer, big.NewInt(1))
	ck.mu.Unlock()

	DPrintf("[Client][GetPutAppend] id:%s op:%s key:%s value:%s", uniqueRequestId, op, key, value)
	for {
		for _, v := range ck.servers {
			args := GetPutAppendArgs{
				Key:             key,
				Value:           value,
				Op:              op,
				UniqueRequestId: uniqueRequestId,
			}
			reply := GetPutAppendReply{
				Err:     "",
				Success: false,
			}
			v.Call("KVServer.GetPutAppend", &args, &reply)
			if reply.Success {
				DPrintf("[Client][GetPutAppend] id:%s op:%s key:%s value:%s return", uniqueRequestId, op, key, value)
				return reply.Value
			}
		}
	}


}

func (ck *Clerk) Put(key string, value string) {
	ck.GetPutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.GetPutAppend(key, value, OpAppend)
}
