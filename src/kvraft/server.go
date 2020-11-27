package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)


const (
	OpPut = "Put"
	OpAppend = "Append"
	OpGet = "Get"
)

type Op struct {
	Type string // Put, Append, Get
	Key string
	Value string
	UniqueRequestId string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	cond *sync.Cond
	kvData map[string]string
	request map[string]string
}

func (kv *KVServer) GetPutAppend(args *GetPutAppendArgs, reply *GetPutAppendReply) {
	// first check if it's a leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		//DPrintf("[%d][%s][Server][GetPutAppend] peer isn't a leader", kv.me, args.UniqueRequestId)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// check repeat(committed)
	v, ok := kv.request[args.UniqueRequestId]
	if ok {
		reply.Err = OK
		reply.Success = true
		reply.Value = v
		kv.mu.Unlock()
		DPrintf("[%d][%s][Server][GetPutAppend] check repeat(committed), return", kv.me, args.UniqueRequestId)
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:  args.Op,
		Key:   args.Key,
		Value: args.Value,
		UniqueRequestId : args.UniqueRequestId,
	}

	// check repeat(uncommitted)
	if kv.rf.CheckReadyToCommit(op) {
		reply.Success = false
		time.Sleep(100 * time.Millisecond)
		DPrintf("[%d][%s][Server][GetPutAppend] check repeat(uncommitted), return", kv.me, args.UniqueRequestId)
		return
	}

	// then commit it and wait for the result
	_, _, isLeader2 := kv.rf.Start(op)
	if !isLeader2 {
		reply.Success = false
		reply.Err = ErrWrongLeader
		DPrintf("[%d][%s][Server][GetPutAppend] peer isn't leader now, return", kv.me, args.UniqueRequestId)
		return
	}

	DPrintf("[%d][%s][Server][GetPutAppend] committed and wait", kv.me, args.UniqueRequestId)

	for {
		_, isLeader3 := kv.rf.GetState()
		if kv.killed() || !isLeader3 {
			DPrintf("[%d][%s][Server][GetPutAppend] peer isn't leader now, return", kv.me, args.UniqueRequestId)
			break
		}
		// wait until kv.request is changed
		kv.mu.Lock()
		v, ok := kv.request[args.UniqueRequestId]
		if ok {
			reply.Err = OK
			reply.Success = true
			reply.Value = v
		}
		kv.mu.Unlock()
		if ok {
			DPrintf("[%d][%s][Server][GetPutAppend] success, return", kv.me, args.UniqueRequestId)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.cond = sync.NewCond(new(sync.Mutex))
	kv.kvData = make(map[string]string)
	kv.request = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	
	go func() {
		for m := range kv.applyCh {
			if m.CommandValid == false {
				// ignore other types of ApplyMsg
			} else {
				//DPrintf("[%d][ApplyCh] cmd: %s", kv.me, m.Command)
				kv.mu.Lock()
				//DPrintf("[%d][ApplyCh] get lock", kv.me)
				v := m.Command.(Op)
				res := ""
				switch v.Type {
				case OpGet:
					{
						res = kv.kvData[v.Key]
						//DPrintf("[%d][ApplyCh] key:%s value:%s", kv.me, v.Key, res)
					}
				case OpPut:
					{
						kv.kvData[v.Key] = v.Value
						//DPrintf("[%d][ApplyCh] key:%s newValue:%s", kv.me, v.Key, kv.kvData[v.Key])
					}
				case OpAppend:
					{
						kv.kvData[v.Key] = kv.kvData[v.Key] + v.Value
						//DPrintf("[%d][ApplyCh] key:%s newValue:%s", kv.me, v.Key, kv.kvData[v.Key])
					}
				}
				kv.request[v.UniqueRequestId] = res
				kv.mu.Unlock()
				//DPrintf("[%d][applyCh] release lock", kv.me)
				//kv.cond.Broadcast() // wake up all waiting goroutines
			}

			if kv.killed() {
				return
			}
		}
	}()

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}