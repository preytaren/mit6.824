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
	LEADER RaftState = iota
	FOLLOWER
	CANDIDATE
	NIL
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type AppendResult struct {
	Term    int
	Success bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
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

type Log struct {
	Term    int
	Command interface{}
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
	return time.Duration(150+rand.Int31n(200)) * time.Millisecond
}

type ReplyTimer struct {
	mu      sync.Mutex
	timer   *time.Timer
	Timeout bool
}

func (tm *ReplyTimer) StartReplyTimer(duration time.Duration) {
	//DPrintf("TIMER %s", duration*time.Millisecond)
	tm.timer = time.NewTimer(duration * time.Millisecond)
}

func (tm *ReplyTimer) Wait() bool {
	tm.mu.Lock()
	tm.Timeout = true
	tm.mu.Unlock()
	<-tm.timer.C
	tm.mu.Lock()
	timeout := tm.Timeout
	tm.mu.Unlock()
	return timeout
}

func (tm *ReplyTimer) ResetReplyTimer(duration time.Duration) {
	tm.mu.Lock()
	tm.Timeout = false
	tm.mu.Unlock()
	tm.timer.Reset(duration * time.Millisecond)
}

func (tm *ReplyTimer) StopReplyTimer() bool {
	tm.mu.Lock()
	tm.Timeout = false
	tm.mu.Unlock()
	return tm.timer.Stop()
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
	state       RaftState
	applyCh     chan ApplyMsg
	stateCh     chan RaftState

	nextIndex  []int // next logIndex send to each server
	matchIndex []int // accepted logIndex by each server

	heartBeatsTimer *time.Timer
	replyTimer      *time.Timer
	replyTimeout    bool
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendResult) {
	rf.mu.Lock()


	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit >= len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	rf.mu.Unlock()

	rf.resetHeartBeatsTimer()

	if args.Entries == nil {
		if rf.state != LEADER{
			rf.mu.Lock()
			reply.Term = rf.currentTerm
			reply.Success = true
			rf.state = FOLLOWER
			rf.currentTerm = args.Term
			rf.mu.Unlock()
			return
		}
	}

	rf.mu.Lock()
	//DPrintf("Node: %d, PrevlogIndex: %d, logs: %s", rf.me, args.PrevLogIndex, rf.logs)
	if args.Term < rf.currentTerm || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		//DPrintf("%d in Term %d APPEND Entry %s to Node %d in Term %d, reply %s", args.LeaderId, args.Term, args.Entries, rf.me, rf.currentTerm, reply)
	} else {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		// append log
		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
		//DPrintf("[APPEND] logs %s ON NODE %d", args.Entries, rf.me)
		rf.currentTerm = args.Term
		reply.Success = true
		reply.Term = rf.currentTerm
	}

	rf.mu.Unlock()
	// DPrintf("111args.PrevlogIndex %d, args.PrevlogTerm %d == rf.prevTerm %d, result %s", args.PrevLogIndex, args.PrevLogTerm, rf.logs[args.PrevLogIndex].Term, reply.Success)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendResult) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer DPrintf("%d(%d|term%d|vote%d) replyed %d(%d) with %s", rf.me, rf.state, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term, reply)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= rf.lastApplied {
		//rf.resetHeartBeatsTimer()

		reply.VoteGranted = true
		// rf.currentTerm += 1
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
		return
	} else {
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

func (rf *Raft) StartVote() bool {
	replyChan := make(chan *RequestVoteReply, len(rf.peers))
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.mu.Unlock()
	//DPrintf("[STARTVOTE] Node %d", rf.me)
	for idx, _ := range rf.peers {
		go func(repl chan *RequestVoteReply, peerId int) {
			var lastTerm int
			if rf.lastApplied >= 1 {
				lastTerm = rf.logs[rf.lastApplied].Term
			} else {
				lastTerm = 0
			}
			rf.mu.Lock()
			args := RequestVoteArgs{rf.currentTerm, rf.me, rf.lastApplied, lastTerm}
			rf.mu.Unlock()
			rep := RequestVoteReply{}
			//DPrintf("%dSend to %d", rf.me, peerId)
			tm := ReplyTimer{}
			tm.StartReplyTimer(time.Duration(30))

			go func(timer ReplyTimer) {
				ok := tm.Wait()
				if ok {
					repl <- nil
				}
			}(tm)

			rf.sendRequestVote(peerId, &args, &rep)
			// DPrintf("%d NORMAL RETURN %s", peerId, rep)
			if tm.StopReplyTimer() {
				// DPrintf("%d NORMAL RETURN %s", peerId, rep)
				repl <- &rep
			}
		}(replyChan, idx)
	}

	count := 0
	nocount := 0
	for each := range replyChan {
		//DPrintf("Node %d EACH %s", rf.me, each)
		if each == nil {
			nocount += 1
		} else if each.VoteGranted {
			count += 1
		} else {
			nocount += 1
		}
		if count > len(rf.peers)/2 {
			//DPrintf("%s was elected", rf.me)
			return true
		} else if nocount > len(rf.peers)/2 {
			//DPrintf("%s election failed", rf.me)
			return false
		}
	}
	//DPrintf("%s election failed", rf.me)
	return false
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
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if state != LEADER {
		return -1, -1, false
	}
	DPrintf("[START COMMAND] %d ON NODE %d TERM %d", command, rf.me, rf.currentTerm)

	index := -1
	term, isLeader := rf.GetState()

	// Your code here (2B).

	log := Log{rf.currentTerm, command}


	rf.mu.Lock()
	index = len(rf.logs)
	rf.logs = append(rf.logs, log)
	rf.matchIndex[rf.me] += 1
	rf.nextIndex[rf.me] += 1
	rf.mu.Unlock()


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

func (rf *Raft) SendHeartBeats() {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	for state == LEADER {
		for peerId, _ := range rf.peers {
			if peerId == rf.me {
				continue
			}
			go func(peerId int) {
				prevLogIndex := 0//+ rf.matchIndex[peerId]
				var prevLogTerm int
				//DPrintf("PREvLOG %d", prevLogIndex)
				rf.mu.Lock()
				prevLogTerm = rf.logs[prevLogIndex].Term
				args := AppendEntriesArgs{rf.currentTerm, rf.me,
					prevLogIndex, prevLogTerm, nil,
					rf.commitIndex}
			    rf.mu.Unlock()
				repl := AppendResult{}
				//DPrintf("ARGS term %d Entries %s prevTerm %d", args.Term, args.Entries, args.PrevLogTerm)
				rf.sendAppendEntries(peerId, &args, &repl)
				rf.mu.Lock()
				if repl.Term > rf.currentTerm {
					rf.currentTerm = repl.Term
					rf.state = FOLLOWER
				}
				rf.mu.Unlock()
			}(peerId)
		}
		sleepTime := time.Duration(30) * time.Millisecond
		time.Sleep(sleepTime)
		rf.mu.Lock()
		state = rf.state
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetHeartBeatsTimer() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = FOLLOWER
	slTime := getSleepTime()
	res := rf.heartBeatsTimer.Reset(slTime)
	return res
}

// is leader, check logs
func (rf *Raft) CheckLogs() {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	for state == LEADER {
		//DPrintf("CHECKLOGS ON NODE %d: logs %s", rf.me, rf.logs)
		//appendChan := make(chan AppendResult, len(rf.peers))
		for peerId := range rf.peers {
			if peerId == rf.me {
				continue
			}
			rf.mu.Lock()
			logLen := len(rf.logs)
			nextIndex := rf.nextIndex[peerId]
			rf.mu.Unlock()
			if logLen > nextIndex {
				go func(peerId int) {
					rf.mu.Lock()
					prevLogIndex := rf.matchIndex[peerId]
					prevLogTerm := rf.logs[prevLogIndex].Term
					args := AppendEntriesArgs{rf.currentTerm, rf.me,
						prevLogIndex, prevLogTerm,
						rf.logs[prevLogIndex+1:], rf.commitIndex}
						//DPrintf("[BEFOREAPPEND] ENTRIES %s PREV %d LOGS %s", args.Entries, args.PrevLogIndex, rf.logs)
					repl := AppendResult{}
					rf.mu.Unlock()
					for rf.state == LEADER {
						rf.sendAppendEntries(peerId, &args, &repl)
						//DPrintf("[CHECKAPPENDENTRIES REPLY]me: %d Term %d send to %d args: %s repl %s", rf.me, rf.currentTerm, peerId, args, repl)
						if repl.Success && rf.state == LEADER{
							rf.mu.Lock()
							rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
							rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
							rf.mu.Unlock()
							break
						}

						rf.mu.Lock()
						if repl.Term > rf.currentTerm {
							rf.currentTerm = repl.Term
							rf.state = FOLLOWER
							break
						}

						if args.PrevLogIndex > 0 {
							args.PrevLogIndex -= 1
							args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
							args.Entries = rf.logs[args.PrevLogIndex+1:]
						}
						rf.mu.Unlock()
					}
				}(peerId)
			}
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
		// sleep for a while
	}
}

func (rf *Raft) checkLeaderCommit() {
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	for state == LEADER {
		rf.mu.Lock()
		counter := make([]int, len(rf.logs) + 1)
		for peerId := range rf.peers{
			counter[rf.matchIndex[peerId]] += 1
		}
		//DPrintf("[COUNTER] NODE %d %s", rf.me, counter)
		rf.mu.Unlock()

		count := 0
		rf.mu.Lock()
		logLen := len(rf.logs)
		commitIndex := rf.commitIndex
		peerLen := len(rf.peers)
		for i := logLen - 1 ; i >= commitIndex; i-- {
			count += counter[i]
			if count > peerLen / 2 && rf.logs[i].Term == rf.currentTerm {
				rf.commitIndex = i
				break
			}
		}
		state = rf.state
		rf.mu.Unlock()
		time.Sleep(time.Duration(30) * time.Millisecond)
	}
}

func (rf *Raft) CheckApply() {
	for {
		rf.mu.Lock()
		//DPrintf("Node %d commitIndex %d > lastapplied %d", rf.me, rf.commitIndex, rf.lastApplied)
		if rf.commitIndex > rf.lastApplied {
			appMsg := ApplyMsg{}
			if rf.currentTerm == rf.logs[rf.lastApplied + 1].Term {
			    appMsg.Index = rf.lastApplied + 1
			    appMsg.Command = rf.logs[rf.lastApplied + 1].Command
			    DPrintf("[APPLYMSG] INDEX: %d, CMD: %d FROM NODE %d TERM %d LEADER %d", appMsg.Index, appMsg.Command, rf.me, rf.currentTerm, rf.votedFor)
			    rf.applyCh <- appMsg
			}
			rf.lastApplied += 1
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(35) * time.Millisecond)
	}
}

type CommitArgs struct {
	Term        int
	CommitIndex int
}

type CommitResult struct {
	Success bool
}


func (rf *Raft) WaitForHBTimer() {
	for {
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
	prev := NIL
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == prev {
			prev = state
			continue
		}
		switch state {
		case CANDIDATE:
			//DPrintf("CheckState me%d:state%d:term%d", rf.me, rf.state, rf.currentTerm)
			rf.mu.Lock()
			rf.votedFor = rf.me
			rf.mu.Unlock()
			ok := rf.StartVote()
			if ok {
				rf.mu.Lock()
				rf.state = LEADER
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				rf.votedFor = -1
				rf.mu.Unlock()
				rf.resetHeartBeatsTimer()
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
			}
		case LEADER:
			DPrintf("[ELECTED]Node %d for Term %d", rf.me, rf.currentTerm)
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = len(rf.logs) - 1
			}
			rf.heartBeatsTimer.Stop()
			go rf.CheckLogs()
			go rf.checkLeaderCommit()
			rf.SendHeartBeats()
		case FOLLOWER:
		}
		prev = state
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	defer DPrintf("[Make Node] %d", me)
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.logs = []Log{}
	rf.logs = append(rf.logs, Log{-1, 0})
	rf.NewTimer()
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.mu.Unlock()

	go rf.WaitForHBTimer()
	// DPrintf("MAKE node%d, with state%d", rf.me, rf.state)

	// initialize from state persisted before a crash
	go rf.CheckApply()
	go rf.CheckState()
	rf.readPersist(persister.ReadRaftState())

	return rf
}
