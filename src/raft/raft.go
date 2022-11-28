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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	heartbeatTimeout = 100
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
	logEntries  []LogEntry
	flushT      chan struct{} //收到append就向heartbeat通道输入一个struct{}
}

type LogEntry struct {
	Term    int
	Command int //todo B
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	if currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state != StateFollower {
			// fmt.Println(rf.me, ":change to follower1")
			rf.state = StateFollower
		}
	}
	// logEntries := rf.logEntries
	rf.mu.Unlock()
	reply.Term = currentTerm
	if args.Term < currentTerm {
		reply.Success = false
		return
	}
	//todo: consistency check!!!
	go func() {
		rf.flushT <- struct{}{}
		// fmt.Println("get a heartbeat")
	}()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
		if rf.state != StateFollower {
			// fmt.Println(rf.me, ":", rf.currentTerm, "<", args.Term)
			// fmt.Println(rf.me, ":change to follower(receive RequestVote)")
		}
		rf.state = StateFollower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	votedfor := rf.votedFor
	id := args.CandidateID
	currentTerm := rf.currentTerm //接收方的term记作currentTerm
	logLength := len(rf.logEntries)
	logTerm := -1
	if logLength > 0 {
		logTerm = rf.logEntries[logLength-1].Term
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
		} else if args.LastLogTerm == logTerm && args.LastLogIndex > logLength-1 {
			reply.VotedGranted = true
		} else if args.LastLogTerm == logTerm && args.LastLogIndex == logLength-1 {
			reply.VotedGranted = true
		} else {
			reply.VotedGranted = false
		}
		if reply.VotedGranted {
			rf.votedFor = args.CandidateID
			//vote for other,reset timer
			go func() {
				rf.flushT <- struct{}{}
			}()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) election() bool {
	rf.mu.Lock()
	rf.currentTerm++
	// fmt.Println(rf.currentTerm)
	rf.state = StateCandidate
	currentTerm := rf.currentTerm
	candidateID := rf.me
	lastLogIndex := len(rf.logEntries) - 1
	lastLogTerm := -1
	if len(rf.logEntries) > 0 {
		lastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
	}
	granted := 1 //vote for itself
	rf.votedFor = rf.me
	rf.mu.Unlock()
	go func() {
		//vote for itself, reset timer
		rf.flushT <- struct{}{}
	}()
	n := len(rf.peers)
	total := len(rf.peers)
	cnt := 1
	vote := make(chan bool, n)
	for i := 0; i < n; i++ {
		if i != rf.me {
			go func(i int) {
				args := RequestVoteArgs{}
				args.CandidateID = candidateID
				args.Term = currentTerm
				args.LastLogIndex = lastLogIndex
				args.LastLogTerm = lastLogTerm
				reply := RequestVoteReply{}
				timeout := make(chan struct{})
				result := make(chan bool)
				go func() {
					time.Sleep(100 * time.Millisecond)
					timeout <- struct{}{}
				}()
				go func() {
					result <- rf.sendRequestVote(i, &args, &reply)
				}()
				select {
				case <-timeout:
					go func() { vote <- false }()
					return
				case outcome := <-result:
					if !outcome {
						go func() { vote <- false }()
						return
					}
				}
				if reply.VotedGranted {
					go func() { vote <- true }()
					return
				} else {
					go func() { vote <- false }()
				}
				rf.mu.Lock()
				newTerm := rf.currentTerm
				if newTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					if rf.state != StateFollower {
						// fmt.Println(rf.me, ":change to follower3")
					}
					rf.state = StateFollower

				}
				rf.mu.Unlock()

			}(i)

		}
	}
	for outcome := range vote {
		rf.mu.Lock()
		newTerm := rf.currentTerm
		rf.mu.Unlock()
		if newTerm != currentTerm {
			return false
		}
		if outcome {
			granted++
		}
		cnt++
		// fmt.Println(granted)
		if granted > total/2 {
			//elected
			// fmt.Println(rf.me, ":elected")
			rf.mu.Lock()
			if rf.state == StateCandidate {
				rf.state = StateLeader
				rf.mu.Unlock()
				go rf.leaderTicker()
				return true
			} else {
				rf.mu.Unlock()
				return false
			}
		}
		if cnt == total {
			return false
		}
	}

	return false
}

// 定时发送heartbeat.
// To be continued in lab2b
func (rf *Raft) leaderTicker() {
	// fmt.Println(rf.me, ":leadertick")
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		logs := rf.logEntries
		currentTerm := rf.currentTerm
		leaderID := rf.me
		n := len(rf.peers)
		rf.mu.Unlock()
		if state != StateLeader {
			return
		}
		for i := 0; i < n; i++ {
			if i != rf.me {
				go func(i int) {
					args := AppendEntriesArgs{}
					args.Entries = logs
					args.LeaderID = leaderID
					args.Term = currentTerm
					reply := AppendEntriesReply{}
					rpcSuccess := rf.sendAppendEntries(i, &args, &reply)
					if !rpcSuccess {
						return
						// fmt.Println(rf.me, "to", i, ":sendAppendEntries,fail")
					}
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						//变为follower
						if rf.state != StateFollower {
							// fmt.Println(rf.me, ":old leader back and change to follower")
						}
						rf.currentTerm = reply.Term
						rf.state = StateFollower
						rf.votedFor = -1
					}
					rf.mu.Unlock()
				}(i)

			}
		}
		time.Sleep(heartbeatTimeout * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// fmt.Println(rf.me, ":ticker start!")
	for !rf.killed() {
		_, isLeader := rf.GetState()
		if isLeader {
			//dosth
			time.Sleep(heartbeatTimeout * time.Millisecond)
			continue
		}
		//candidate or follower
		rand.Seed(time.Now().UnixNano())
		randNum := rand.Intn(200) + 600
		select {
		case <-time.After(time.Duration(randNum) * time.Millisecond):
			//become a candidate
			// outcome := rf.election()
			// fmt.Println(rf.me, ":election ", outcome)
			rf.election()
		case <-rf.flushT:
			//flush clock: leader heartbeat(AppendEntries RPC),vote for somebody
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = StateFollower
	rf.votedFor = -1
	rf.flushT = make(chan struct{})
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
