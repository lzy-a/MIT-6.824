package shardctrler

import (
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

// lab4
type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	Debug bool

	waitChMap map[int]chan Op
	seqMap    map[int64]int
	configs   []Config // indexed by config num
}

type Op struct {
	// Your data here.
	OpType       string
	SeqId        int
	ClientId     int64
	Conf         Config
	NewMapping   map[int][]string
	RemoveGroups []int //GIDs
	MoveShard    int
	MoveGID      int
	QueryNum     int
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}
	return ch
}
func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	seq, exist := sc.seqMap[clientId]
	if !exist {
		return false
	}
	return seq >= seqId
}

// The shardctrler should react by creating a new configuration that includes the new replica groups.
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		OpType:     "Join",
		SeqId:      args.SeqId,
		ClientId:   args.ClintId,
		NewMapping: args.Servers,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	ch := sc.getWaitCh(index)
	defer func() {
		sc.mu.Lock()
		close(ch)
		delete(sc.waitChMap, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		//
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}

}

// The shardctrler should create a new configuration that does not include
// those groups, and that assigns those groups' shards to the remaining groups.
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpType:       "Leave",
		SeqId:        args.SeqId,
		ClientId:     args.ClintId,
		RemoveGroups: args.GIDs,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	ch := sc.getWaitCh(index)
	defer func() {
		sc.mu.Lock()
		close(ch)
		delete(sc.waitChMap, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		//
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

// The shardctrler should create a new configuration in which the shard is assigned to the group.
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpType:    "Move",
		SeqId:     args.SeqId,
		ClientId:  args.ClintId,
		MoveShard: args.Shard,
		MoveGID:   args.GID,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	ch := sc.getWaitCh(index)
	defer func() {
		sc.mu.Lock()
		close(ch)
		delete(sc.waitChMap, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		//
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		OpType:   "Query",
		SeqId:    args.SeqId,
		ClientId: args.ClintId,
		QueryNum: args.Num,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	ch := sc.getWaitCh(index)
	defer func() {
		sc.mu.Lock()
		close(ch)
		delete(sc.waitChMap, index)
		sc.mu.Unlock()
	}()
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case replyOp := <-ch:
		//
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
			reply.Config = replyOp.Conf
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) applyMsgHandlerLoop() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				op := msg.Command.(Op)
				index := msg.CommandIndex
				if !sc.ifDuplicate(op.ClientId, op.SeqId) {
					sc.mu.Lock()
					config := Config{
						Num:    len(sc.configs),
						Shards: [NShards]int{},
						Groups: map[int][]string{},
					}
					op.Conf = config
					if op.OpType == "Join" {
						//need NewMapping
					} else if op.OpType == "Leave" {
						//need RemoveGroups
					} else if op.OpType == "Move" {
						//need MoveShard MoveGID
					} else if op.OpType == "Query" {
						if op.QueryNum == -1 {
							op.Conf = sc.configs[len(sc.configs)-1]
						} else if op.QueryNum < len(sc.configs) {
							op.Conf = sc.configs[op.QueryNum]
						} else {
							sc.DPrintf("===applyMsgHandlerLoop===server %d,query out of bound!", sc.me)
						}
					}
					sc.mu.Unlock()
				} else {
					sc.DPrintf("===applyMsgHandlerLoop===server %d msg, index:%d, opType:%s,Duplicate!!!", sc.me, index, op.OpType)
				}
				_, isLeader := sc.rf.GetState()
				if isLeader {
					ch := sc.getWaitCh(index)
					ch <- op
				}

			}
		}
	}
}

func (sc *ShardCtrler) DPrintf(format string, a ...interface{}) (n int, err error) {
	if sc.Debug {
		log.Printf(format, a...)
	}
	return
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	sc.DPrintf("%d killed", sc.me)
	sc.Debug = false
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	if Debug {
		sc.Debug = true
	}
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)
	go sc.applyMsgHandlerLoop()
	return sc
}
