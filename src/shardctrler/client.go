package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

// lab4
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId   int64
	seqId      int
	lastLeader int
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
	// Your code here.
	ck.clientId = nrand()
	ck.seqId = 0
	ck.lastLeader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.seqId++
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClintId = ck.clientId
	args.SeqId = ck.seqId
	for {
		// try each known server.
		srv := ck.servers[ck.lastLeader]
		var reply QueryReply
		ok := srv.Call("ShardCtrler.Query", args, &reply)
		if ok && !reply.WrongLeader {
			return reply.Config
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seqId++
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClintId = ck.clientId
	args.SeqId = ck.seqId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqId++
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClintId = ck.clientId
	args.SeqId = ck.seqId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqId++
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClintId = ck.clientId
	args.SeqId = ck.seqId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
