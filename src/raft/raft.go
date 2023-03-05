package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
const (
	StateFollower    = 1
	StateCandidate   = 2
	StateLeader      = 3
	heartbeatTimeout = 35
	AppliedSleep     = 10
)

const (
	Normal int = iota //投票过程正常
	Killed            //Raft节点已终止
	Expire            //投票(消息\竞选者）过期
	Voted             //本Term内已经投过票

)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int
	currentTerm int
	votedFor    int
	voteNum     int
	votedTimer  time.Time

	logEntries []LogEntry

	applyCh chan ApplyMsg

	commitIndex       int
	lastApplied       int
	lastIncludedIndex int
	lastIncludedTerm  int

	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term int
	// Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader :return term, isleader
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	isleader = rf.state == StateLeader
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}
func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("Decode error!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntries = logEntries
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.persist()
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// Done              bool
	// Offset            int
}
type InstallSnapshotReply struct {
	Term int
}

// Invoked by leader to send chunks of a snapshot to a follower. Leaders always send chunks in order.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

	rf.mu.Lock()
	DPrintf("---InstallSnapshot---%d", rf.me)
	reply.Term = rf.currentTerm
	//Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = args.Term
	rf.state = StateFollower
	rf.voteNum = 0
	rf.votedFor = -1
	rf.persist()
	rf.votedTimer = time.Now()
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	tmpLog := []LogEntry{{}}
	for i := args.LastIncludedIndex + 1; i <= rf.getLastIndex(); i++ {
		tmpLog = append(tmpLog, rf.restoreLog(i))
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.logEntries = tmpLog
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}

	rf.applyCh <- msg
	rf.mu.Unlock()
}

func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()
	DPrintf("---leaderSendSnapShot---%d to %d", rf.me, server)
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	DPrintf("===leaderSendSnapShot===leader:%d,rf:%d,LastIncludedIndex:%d", rf.me, server, rf.lastIncludedIndex)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		DPrintf("snapshot rpc failed!")
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != StateLeader || rf.currentTerm != args.Term {
		return
	}
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.votedTimer = time.Now()
		rf.persist()
		return
	}
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = args.LastIncludedIndex + 1
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// The index argument indicates the highest log entry that's reflected in the snapshot.
// leader制作snapshot
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("---snapshot---%d,lastIncludedIndex:%d", rf.me, index)
	if rf.lastIncludedIndex >= index || index > rf.commitIndex {
		return
	}
	sLog := []LogEntry{{}}
	// sLog = append(sLog, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLog = append(sLog, rf.restoreLog(i))
	}
	if index == rf.getLastIndex()+1 {
		fmt.Println("what happended")
		rf.lastIncludedTerm = rf.getLastTerm()
	} else {
		rf.lastIncludedTerm = rf.restoreLogTerm(index)
	}

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.lastIncludedIndex = index
	rf.logEntries = sLog
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

const (
	AppNormal    int = iota // 追加正常
	AppOutOfDate            // 追加过时
	AppKilled               // Raft程序终止
	AppCommitted            // 追加的日志已经提交 (2B
	Mismatch                // 追加不匹配 (2B

)

type AppendEntriesReply struct {
	Term        int
	Success     bool
	AppState    int
	UpNextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("===AppendEntries===leader:%d,rf:%d,term:%d", args.LeaderID, rf.me, args.Term)
	reply.UpNextIndex = -1
	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = -1
		reply.Success = false
		return
	}
	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Success = false
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.state = StateFollower
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	//consistency check!!!
	//1、 如果preLogIndex的大于当前日志的最大的下标说明跟随者缺失日志，拒绝附加日志
	//2、 如果preLog处的任期和preLogIndex处的任期和preLogTerm不相等，那么说明日志存在conflict,拒绝附加日志
	if rf.lastIncludedIndex > args.PrevLogIndex {
		reply.Success = false
		reply.AppState = Mismatch
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	}

	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.AppState = Mismatch
		// reply.UpNextIndex = rf.getLastIndex()
		reply.UpNextIndex = rf.lastApplied + 1
		return
	} else {
		if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			reply.AppState = Mismatch
			reply.UpNextIndex = rf.lastApplied + 1
			// reply.UpNextIndex = rf.getLastIndex()
			// tmpTerm := rf.restoreLogTerm(args.PrevLogIndex)
			// for index := args.PrevLogIndex; index >= rf.lastIncludedIndex; index-- {
			// 	if rf.restoreLogTerm(index) != tmpTerm {
			// 		reply.UpNextIndex = index + 1
			// 		break
			// 	}
			// }
			return
		}
	}

	if args.PrevLogIndex > 0 && (rf.getLastIndex() < args.PrevLogIndex ||
		rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm) {
		reply.Success = false
		reply.AppState = Mismatch
		reply.UpNextIndex = rf.lastApplied + 1
		DPrintf("===func AppendEntries===leader:%d to %d Mismatch. prevLogIndex:%d,receiver.logsLen:%d", args.LeaderID, rf.me, args.PrevLogIndex, len(rf.logEntries)-1)
		return
	}

	//?为什么会有这种情况
	if args.PrevLogIndex != -1 && rf.lastApplied > args.PrevLogIndex {
		reply.AppState = AppCommitted
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = rf.lastApplied + 1
		DPrintf("rf.lastApplied:%d > args.PrevLogIndex%d", rf.lastApplied, args.PrevLogIndex)
		return
	}

	//我想的是把长的log裁剪一下

	rf.logEntries = append(rf.logEntries[:args.PrevLogIndex+1-rf.lastIncludedIndex], args.Entries...)
	rf.persist()

	// If leaderCommit > commitIndex, set commitIndex =min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("--sendAppendEntries--from:%dto%d", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			// DPrintf("%d was killed", rf.me)
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//处理返回值

	DPrintf("Reply from%dto%d appstate:%d,success:%v", server, rf.me, reply.AppState, reply.Success)
	if reply.Term > rf.currentTerm {
		//变为follower
		DPrintf("===func sendAppendEntries===leader:%d->follower,replyTerm:%d,currentTerm:%d", rf.me, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.state = StateFollower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.votedTimer = time.Now()
		rf.persist()
		return
	}

	switch reply.AppState {
	case AppKilled:
		{
			DPrintf("===func sendAppendEntries===leader:%d server %d->killed", rf.me, server)
			return
		}
	case AppNormal:
		{
			DPrintf("===func sendAppendEntries===reply:appNormal")
			if reply.Success {
				rf.commitIndex = rf.lastApplied
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			}
			// DPrintf("lastapplied+1:%d,len(logEntries):%d", rf.lastApplied+1, len(rf.logEntries))
			canApplyIndex := rf.lastApplied
			// DPrintf("%d %d", rf.getLastIndex(), rf.lastApplied)
			for i := rf.getLastIndex(); i > rf.lastIncludedIndex; i-- {

				if rf.restoreLogTerm(i) == rf.currentTerm {
					cnt := 1
					for j := 0; j < len(rf.peers); j++ {
						if j != rf.me && rf.matchIndex[j] >= i {
							cnt++
						}
					}
					if cnt > len(rf.peers)/2 {
						DPrintf("===logs=== index:%d,cnt:%d", i, cnt)
						canApplyIndex = i
						break
					}
				}
			}
			rf.commitIndex = canApplyIndex
		}
	case Mismatch, AppCommitted:
		{
			rf.nextIndex[server] = reply.UpNextIndex
		}
	}
	DPrintf("leader %d lastapplied:%d", rf.me, rf.lastApplied)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.state = StateFollower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteNum = 0
		// rf.votedTimer = time.Now()
		rf.persist()
	}
	votedfor := rf.votedFor
	id := args.CandidateID
	currentTerm := rf.currentTerm //接收方的term记作currentTerm
	logLength := rf.getLastIndex()
	logTerm := -1
	if logLength > 0 {
		logTerm = rf.getLastTerm()
	}
	defer rf.mu.Unlock()
	term := args.Term //发送方的term
	reply.Term = currentTerm
	reply.VotedGranted = false
	if term < currentTerm {
		reply.VotedGranted = false
		return
	}

	if votedfor == -1 || votedfor == id {
		//发送方的log more or equal to up-to-date
		// fmt.Println(args.LastLogTerm, logTerm, args.LastLogIndex, logLength)
		if args.LastLogTerm > logTerm {
			reply.VotedGranted = true
		} else if args.LastLogTerm == logTerm && args.LastLogIndex > logLength {
			reply.VotedGranted = true
		} else if args.LastLogTerm == logTerm && args.LastLogIndex == logLength {
			reply.VotedGranted = true
		} else {
			reply.VotedGranted = false
		}
		if reply.VotedGranted {
			rf.votedFor = args.CandidateID
			//vote for other,reset timer
			rf.votedTimer = time.Now()
			rf.persist()
		}
	}
	// reply.VotedGranted = false

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.killed() {
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.getLastIndex() + 1
	//+1?

	term := rf.currentTerm
	isLeader := rf.state == StateLeader

	// Your code here (2B).
	if !isLeader {
		return index, term, isLeader
	}
	rf.logEntries = append(rf.logEntries, LogEntry{Term: term, Command: command})
	// go rf.leaderHeartBeatOnce()
	rf.persist()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendElection() {
	rf.currentTerm++
	rf.state = StateCandidate
	rf.votedFor = rf.me
	rf.voteNum = 1
	rf.votedTimer = time.Now()
	rf.persist()
	currentTerm := rf.currentTerm
	candidateID := rf.me
	lastLogIndex := rf.getLastIndex()
	lastLogTerm := -1
	if len(rf.logEntries) > 0 {
		lastLogTerm = rf.getLastTerm()
	}
	n := len(rf.peers)
	total := len(rf.peers)
	DPrintf("%d start election, term:%d\n", rf.me, currentTerm)
	for i := 0; i < n; i++ {
		if i != rf.me {
			go func(i int) {
				args := RequestVoteArgs{
					Term:         currentTerm,
					CandidateID:  candidateID,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := RequestVoteReply{}
				res := rf.sendRequestVote(i, &args, &reply)
				if res {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					newTerm := rf.currentTerm
					//判断自己是否还在竞选
					if rf.state != StateCandidate || args.Term < newTerm {
						return
					}
					//此时确定rf为candidate且args.Term == newTerm
					//接下来判断是否返回者term更大(rf为网络分区回来的机器)
					if newTerm < reply.Term {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.voteNum = 0
						rf.state = StateFollower
						rf.votedTimer = time.Now()
						rf.persist()
						return
					}

					if reply.VotedGranted {
						//得票数+1
						rf.voteNum++
						if rf.voteNum > total/2 {
							rf.state = StateLeader
							rf.votedFor = -1
							rf.voteNum = 0
							rf.votedTimer = time.Now()
							rf.persist()
							rf.leaderInit()
							DPrintf("%d becomes a Leader, term:%d", rf.me, rf.currentTerm)
						}
					}
					// else {
					// 	rf.state = StateFollower
					// 	rf.votedFor = -1
					// 	rf.voteNum = 0
					// 	rf.votedTimer = time.Now()
					// 	rf.persist()
					// }

				}
			}(i)

		}
	}
}

func (rf *Raft) leaderHeartBeatOnce() {
	// fmt.Println(rf.me, ":leadertick")

	rf.mu.Lock()
	DPrintf("%d starts to appendEntries.term:%d ,commitIndex:%d,logsLen:%d", rf.me, rf.currentTerm, rf.commitIndex, len(rf.logEntries)-1)
	// matchIndex:=rf.matchIndex
	n := len(rf.peers)
	rf.mu.Unlock()
	if rf.state != StateLeader {
		return
	}
	for i := 0; i < n; i++ {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				if rf.nextIndex[i] <= rf.lastIncludedIndex {
					rf.mu.Unlock()
					go rf.leaderSendSnapShot(i)
					return
				}
				prevLogIndex, prevLogTerm := rf.getPrevLogInfo(i)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				//nextIndex初始为1+len(logEntries)
				// fmt.Println("nextIndex:", nextIndex[i]-1, ",len(log):", len(rf.logEntries))
				if rf.getLastIndex() >= rf.nextIndex[i] {
					entries := make([]LogEntry, 0)
					entries = append(entries, rf.logEntries[rf.nextIndex[i]-rf.lastIncludedIndex:]...)
					args.Entries = entries
				} else {
					args.Entries = []LogEntry{}
				}

				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				// DPrintf("to%d", i)
				rf.sendAppendEntries(i, &args, &reply)
			}(i)
		}
	}

}

// 定时发送heartbeat.
func (rf *Raft) appendTicker() {
	for !rf.killed() {
		time.Sleep(heartbeatTimeout * time.Millisecond)
		rf.mu.Lock()
		if rf.state == StateLeader {
			rf.mu.Unlock()
			rf.leaderHeartBeatOnce()
		} else {
			rf.mu.Unlock()
		}
	}
	DPrintf("%d was killed", rf.me)
}

// when rf first become a leader, init nextIndex,matchIndex
func (rf *Raft) leaderInit() {
	n := len(rf.peers)
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = 1 + rf.getLastIndex()
	}
	rf.matchIndex = make([]int, n)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) electionTicker() {
	// fmt.Println(rf.me, ":ticker start!")
	for !rf.killed() {
		rand.Seed(time.Now().UnixNano())
		randNum := rand.Intn(100) + 75
		nowTime := time.Now()
		time.Sleep(time.Millisecond * time.Duration(randNum))
		rf.mu.Lock()
		if rf.votedTimer.Before(nowTime) && rf.state != StateLeader {
			rf.sendElection()
			// rf.votedTimer = time.Now()
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) committedTicker() {
	// put the committed entry to apply on the status machine
	for !rf.killed() {
		time.Sleep(AppliedSleep * time.Millisecond)
		rf.mu.Lock()

		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				CommandIndex:  rf.lastApplied,
				Command:       rf.restoreLog(rf.lastApplied).Command,
			})
			DPrintf("===logs===rf:%d  index:%d,start to apply", rf.me, rf.lastApplied)
		}
		rf.mu.Unlock()

		for _, messages := range Messages {
			rf.applyCh <- messages
		}
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.currentTerm = 0
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = StateFollower
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.votedTimer = time.Now()
	rf.voteNum = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1
	rf.logEntries = []LogEntry{{}}
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}
	go rf.electionTicker()
	go rf.appendTicker()
	go rf.committedTicker()
	DPrintf("%d start\n", rf.me)
	return rf
}
