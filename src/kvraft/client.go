package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	seqId      int
	clintId    int64
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
	ck.seqId = 0
	ck.clintId = nrand()
	// You'll have to add code here.
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqId++
	args := GetArgs{key, ck.clintId, ck.seqId}
	i := ck.lastLeader
	DPrintf("clinet %d GET %s. seqId:%d", ck.clintId, key, ck.seqId)
	for {
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				i = (i + 1) % len(ck.servers)
				continue
			} else if reply.Err == ErrNoKey {

			} else if reply.Err == OK {
				ck.lastLeader = i
				return reply.Value
			}
		}
		i = (i + 1) % len(ck.servers)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// fmt.Println(value)
	ck.seqId++
	args := PutAppendArgs{key, value, op, ck.clintId, ck.seqId}
	i := ck.lastLeader
	DPrintf("clinet %d PUTAPPEND key:%s, value:%s. seqId:%d", ck.clintId, key, value, ck.seqId)
	for {
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == ErrWrongLeader {
				i = (i + 1) % len(ck.servers)
				continue
			} else if reply.Err == OK {
				ck.lastLeader = i
				return
			}
		}
		i = (i + 1) % len(ck.servers)

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
