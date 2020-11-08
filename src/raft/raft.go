package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A definition for time gap
const (
	MinElectionTimeOut int64 = 150 // 0.2s
	MaxElectionTimeOut int64 = 500 // 0.5s
	HeatBeatRate int64 = 110 // 0.15s
)

//
// A definition for Raft type
//
type RoleType int32
const (
	RoleLeader    RoleType = 0
	RoleCandidate RoleType = 1
	RoleFollower  RoleType = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// All server
	role RoleType
	lastHeatBeatTime time.Time
	applyCh chan ApplyMsg

	currentTerm int
	voteFor int
	log []LogEntry

	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// As Candidate
	candidateVoteCount int

	// As Leader
	nextIndex []int // for each server, Index of the next log entry to send to that server
	matchIndex []int // for each server, Index of highest log entry known to be replicated on server
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.role == RoleLeader
	DPrintf("[%d][GetState] Term:%d role:%d", rf.me, term, rf.role)
	rf.mu.Unlock()

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// PickWork:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// PickWork:
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

// when a candidate or leader receive a Term message larger than itself, it convert to follower
func (rf *Raft) tryConvertToFollower(me int, term int, nTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if nTerm > rf.currentTerm {
		DPrintf("[%d][tryConvertToFollower] do follower", me)
		rf.currentTerm = nTerm
		rf.role = RoleFollower
		rf.voteFor = -1
	}
}

// As follower If election timeout elapses without receiving AppendEntries
// RPC from current leader or granting vote to candidate:
// convert to candidate
// As candidate, if election timeout elapses: start new election
func (rf *Raft) tryConvertToCandidate(me int) {
	for {
		rf.mu.Lock()
		if rf.role == RoleLeader {
			rf.mu.Unlock()
			continue
		}

		electionTimeout := rand.Int63n(MaxElectionTimeOut - MinElectionTimeOut) + MinElectionTimeOut
		lastHeartBeatTime := rf.lastHeatBeatTime
		term := rf.currentTerm
		rf.mu.Unlock()

		DPrintf("[%d][tryConvertToCandidate] sleep for(electionTimeout): %d", me, electionTimeout)

		time.Sleep(time.Millisecond * time.Duration(electionTimeout))

		rf.mu.Lock()

		if rf.killed() {
			return
		}

		if rf.currentTerm > term || rf.role == RoleLeader || !rf.lastHeatBeatTime.Equal(lastHeartBeatTime){
			DPrintf("[%d][tryConvertToCandidate] give up: %d", me, electionTimeout)
			rf.mu.Unlock()
			continue
		}

		if rf.lastHeatBeatTime.Equal(lastHeartBeatTime) {
			DPrintf("[%d][tryConvertToCandidate] try candidate(wake up): %d", me, electionTimeout)
			// On conversion to candidate, start election:
			//• Increment currentTerm
			//• Vote for self
			//• Reset election timer
			//• Send RequestVote RPCs to all other servers
			rf.role = RoleCandidate
			rf.voteFor = me
			rf.currentTerm = rf.currentTerm + 1
			rf.lastHeatBeatTime = time.Now()
			go rf.gatherVotes(me, rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.role == RoleLeader

	if !isLeader {
		return index, term, isLeader
	}

	// is leader
	index = len(rf.log)
	term = rf.currentTerm
	DPrintf("[%d][Start] new log with index: %d term: %d", rf.me, index, term)

	// If Command received from client: append entry to local log,
	//respond after entry applied to state machine (§5.3)
	entry := LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}

	rf.mu.Lock()
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.nextIndex[rf.me] = len(rf.log)
	rf.mu.Unlock()

	//go rf.sendNewLogToClient(rf.me, term)

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.mu.Unlock()
	DPrintf("[%d][Kill] killed", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{}
	rf.voteFor = -1
	rf.currentTerm = 0
	rf.lastHeatBeatTime = time.Now()
	rf.role = RoleFollower
	rf.candidateVoteCount = 0

	// append an default entry to avoid boundary error
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{
		Term:  0,
		Index: 0,
	})
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("[%d][Make] start", me)
	go rf.tryConvertToCandidate(me)

	return rf
}
