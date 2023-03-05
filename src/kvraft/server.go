package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int // raft服务层传来的Index
	OpType   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap    map[int64]int   //为了确保seq只执行一次	clientId / seqId
	waitChMap map[int]chan Op //传递由下层Raft服务的appCh传过来的command	index / chan(Op)
	kvMap     map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		SeqId:    args.SeqId,
		Key:      args.Key,
		ClientId: args.ClintId,
		OpType:   "Get",
	}
	lastIndex, _, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvMap[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("===PutAppend===kv:%d,ErrWrongLeader", kv.me)
		return
	}
	op := Op{
		SeqId:    args.SeqId,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClintId,
		OpType:   args.Op,
	}
	lastIndex, _, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()
	timer := time.NewTicker(80 * time.Millisecond)
	defer timer.Stop()

	select {
	case replyOp := <-ch:
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader
			DPrintf("===PutAppend===kv:%d,uequal -> ErrWrongLeader", kv.me)
		} else {
			reply.Err = OK
			DPrintf("===PutAppend===kv:%d,seqId:%d,clientId:%d,OK", kv.me, op.SeqId, op.ClientId)
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		DPrintf("===PutAppend===kv:%d,timeout -> ErrWrongLeader", kv.me)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op, 1)
		ch = kv.waitChMap[index]
	}
	return ch
}
func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() {
			return
		}
		select {
		case msg := <-kv.applyCh:
			index := msg.CommandIndex
			op := msg.Command.(Op)
			if msg.CommandValid {
				DPrintf("===applyMsgHandlerLoop===kvserver %d msg, index:%d,seqId:%d, opType:%s", kv.me, index, op.SeqId, op.OpType)
				if !kv.ifDuplicate(op.ClientId, op.SeqId) {
					kv.mu.Lock()
					if op.OpType == "Put" {
						kv.kvMap[op.Key] = op.Value
					} else if op.OpType == "Append" {
						kv.kvMap[op.Key] += op.Value
					}
					kv.seqMap[op.ClientId] = op.SeqId
					kv.mu.Unlock()
				} else {
					// DPrintf("===applyMsgHandlerLoop===kvserver %d msg, index:%d, opType:%s,Duplicate!!!", kv.me, index, op.OpType)
				}
			}
			if msg.SnapshotValid {
				//todo:snapshot
			}
			kv.getWaitCh(index) <- op
		}
	}
}
func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.seqMap = make(map[int64]int)
	kv.waitChMap = make(map[int]chan Op)
	go kv.applyMsgHandlerLoop()
	return kv
}
