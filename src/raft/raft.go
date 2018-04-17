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

import "sync"
import "labrpc"
// import "persister"
import "time"
import (
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

type RaftState int

const (
	LEADER  RaftState = iota
	FOLLOWER
	CANDIDATE
)


type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type AppendResult struct {
	mu sync.Mutex
	Term    int
	Success bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendResult)  {
	defer DPrintf("%d send AppendEntries to %d, %d", args.LeaderId, rf.me, rf.state)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == CANDIDATE {
		if args.Term >= rf.currentTerm {
			rf.state = FOLLOWER
		}
	}
	if rf.state == FOLLOWER {
		rf.resetHeartBeatsTimer()
		if args.Term < rf.currentTerm || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Success = false
		} else {
			// append log
			rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
			rf.currentTerm = args.Term
			reply.Success = true
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit >= len(rf.logs)-1 {
				rf.commitIndex = len(rf.logs) - 1
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) resetHeartBeatsTimer() bool {
	DPrintf("Reset: %s", rf.me)
	return rf.heartBeatsTimer.Reset(getSleepTime())
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendResult) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type Log struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // Current term this peer in
	votedFor    int
	logs        []Log
	commitIndex int
	lastApplied int
	state RaftState
	applyCh chan ApplyMsg

	nextIndex  []int
	matchIndex []int

	heartBeatsTimer *time.Timer
}

func (rf *Raft) NewTimer() {
	t := getSleepTime()
	// DPrintf("Sleep Time of %s : %s", rf.me, t)
	rf.heartBeatsTimer = time.NewTimer(t)
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool

	// Your code here (2A).

	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer DPrintf("%d(%d|term%d|vote%d) replyed %d(%d) with %s", rf.me, rf.state, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term, reply)
	rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		DPrintf("1")
		reply.VoteGranted = false
		return
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= rf.lastApplied {
		DPrintf("2")
		//rf.resetHeartBeatsTimer()

		reply.VoteGranted = true
		rf.mu.Lock()
		// rf.currentTerm += 1
		rf.currentTerm = reply.Term
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
		rf.mu.Unlock()
		return
	} else {
		DPrintf("3")
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// DPrintf("%d send to Server %d", args.CandidateId, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func getSleepTime() time.Duration {
	return time.Duration(150 + rand.Int31n(250)) * time.Millisecond
}

func (rf *Raft) StartVote() bool {
	DPrintf("STARTVOTE %d", rf.me)
	replyChan := make(chan RequestVoteReply, len(rf.peers))
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.mu.Unlock()
	// DPrintf("#%s StartVote", rf.me)
	for idx, _ := range rf.peers {
		go func(repl chan RequestVoteReply, peerId int) {
			var lastTerm int
			if rf.lastApplied >= 1 {
				lastTerm = rf.logs[rf.lastApplied].Term
			}else{
				lastTerm = 0
			}
			args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, lastTerm}
			rep := RequestVoteReply{}
			//DPrintf("%dSend to %d", rf.me, peerId)
			rf.sendRequestVote(peerId, &args, &rep)
			repl <- rep
		}(replyChan, idx)

		// count follower
	}
	count := 0
	for each := range replyChan {
		if each.VoteGranted {
			count += 1
			// DPrintf("GRANTED")
		}
		if count > len(rf.peers) / 2 {
			DPrintf("%s was elected", rf.me)
			//rf.StartHeartBeats()
			return true
		}
	}
	rf.state = FOLLOWER
	return false
}

func (rf *Raft) StartHeartBeats() {
	for rf.state == LEADER {
		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}

			// appendChan := make(chan AppendResult)
			args := AppendEntriesArgs{rf.currentTerm, rf.me,
				rf.lastApplied, rf.logs[rf.lastApplied].Term,
				rf.logs[rf.lastApplied+1:], rf.commitIndex}
			repl := AppendResult{}
			go func(peerId int) {
				rf.sendAppendEntries(peerId, &args, &repl)
				//args.PrevLogIndex -= 1
				//args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				//args.Entries = rf.logs[args.PrevLogIndex+1:]
			}(idx)
		}

		sleepTime := getSleepTime() / 2
		time.Sleep(sleepTime)
	}
}

func (rf *Raft) WaitForHBTimer() {
	for {
		//DPrintf("START WAITING %s", rf.me)
		<-rf.heartBeatsTimer.C
		//DPrintf("Timeout %s %s", rf.me, rf.state)

		rf.mu.Lock()
		rf.state = CANDIDATE
		rf.votedFor = -1
		rf.mu.Unlock()
		//rf.heartBeatsTimer.Stop()
	}
}

func (rf *Raft) CheckState() {
	DPrintf("rf%d State %d", rf.me, rf.state)
	for {
		switch rf.state{
		case CANDIDATE:
			// rf.heartBeatsTimer.Stop()
			rf.mu.Lock()
			rf.votedFor = rf.me
			rf.mu.Unlock()
			ok := rf.StartVote()
			rf.mu.Lock()
			if ok {
				rf.votedFor = rf.me
				rf.state = LEADER
			} else {
				rf.votedFor = -1
				rf.state = FOLLOWER
			}
			rf.mu.Unlock()
			break
		case LEADER:
			//DPrintf("CHECKSTATE node%d, with state%d", rf.me, rf.state)
			DPrintf("LEADER%d", rf.me, rf.state)
			rf.heartBeatsTimer.Stop()
			rf.StartHeartBeats()
			break
		case FOLLOWER:
			// rf.heartBeatsTimer.Reset(getSleepTime())
			break
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	DPrintf("INIT!! %s", me)
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.logs = make([]Log, 1)
	rf.NewTimer()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.mu.Unlock()

	go rf.WaitForHBTimer()
	// DPrintf("MAKE node%d, with state%d", rf.me, rf.state)

	// initialize from state persisted before a crash
	go rf.CheckState()
	rf.readPersist(persister.ReadRaftState())

	return rf
}