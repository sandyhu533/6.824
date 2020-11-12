package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

import "sync/atomic"
import "../labrpc"
import "../labgob"


//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
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
	MinElectionTimeOut int64 = 120 // 0.2s
	MaxElectionTimeOut int64 = 200 // 0.5s
	HeatBeatRate int64 = 100 // 0.15s
)

// A definition for Raft role type
type RoleType string
const (
	RoleLeader    RoleType = "Leader"
	RoleCandidate RoleType = "Candidate"
	RoleFollower  RoleType = "Follower"
)

// A definition for Raft role transfer event
type EventType string
const (
	ElectionTimeOutEvent EventType = "ElectionTimeOutEvent"
	OutDatedEvent EventType = "OutDatedEvent"
	WinElectionEvent EventType = "WinElectionEvent"
	NewLeaderElectedEvent EventType = "NewLeaderElectedEvent"
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
	cond *sync.Cond

	commitIndex int // Index of highest Log entry known to be committed
	lastApplied int // Index of highest Log entry applied to state machine

	// Persistent state
	CurrentTerm int
	VoteFor     int
	Log         []LogEntry

	// As Candidate
	candidateVoteCount int

	// As Leader
	nextIndex []int  // for each server, Index of the next Log entry to send to that server
	matchIndex []int // for each server, Index of highest Log entry known to be replicated on server
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	rf.mu.Lock()
	term = rf.CurrentTerm
	isLeader = rf.role == RoleLeader
	DPrintf("[%d][%s][%d][GetState] Term:%d role:%s", rf.me, rf.role, rf.CurrentTerm, term, rf.role)
	rf.mu.Unlock()

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.Log)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

	DPrintf("[%d][%s][%d][persist] successfully persist, log: %s, currentTerm: %d,voteFor: %d",
		rf.me, rf.role, rf.CurrentTerm, serialize(rf.Log), rf.CurrentTerm, rf.VoteFor)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var Log []LogEntry
	var CurrentTerm int
	var VoteFor int
	if d.Decode(&Log) != nil ||
		d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VoteFor) != nil {
		DPrintf("[%d][%s][%d][readPersist] error when decoding", rf.me, rf.role, rf.CurrentTerm)
	} else {
		rf.Log = Log
		rf.CurrentTerm = CurrentTerm
		rf.VoteFor = VoteFor

		DPrintf("[%d][%s][%d][readPersist] successfully read persist, log: %s, currentTerm: %d,voteFor: %d",
			rf.me, rf.role, rf.CurrentTerm, serialize(rf.Log), rf.CurrentTerm, rf.VoteFor)
	}
}

// As follower If election timeout elapses without receiving AppendEntries
// RPC from current leader or granting vote to candidate:
// convert to candidate
// As candidate, if election timeout elapses: start new election
func (rf *Raft) ElectionTimer() {
	for {
		rf.mu.Lock()
		electionTimeout := rand.Int63n(MaxElectionTimeOut - MinElectionTimeOut) + MinElectionTimeOut
		lastHeartBeatTime := rf.lastHeatBeatTime
		rf.mu.Unlock()

		DPrintf("[%d][%s][%d][ElectionTimer] sleep for(electionTimeout): %d", rf.me, rf.role, rf.CurrentTerm, electionTimeout)

		time.Sleep(time.Millisecond * time.Duration(electionTimeout))

		if rf.killed() {
			return
		}

		rf.mu.Lock()

		if rf.role == RoleLeader || !rf.lastHeatBeatTime.Equal(lastHeartBeatTime) {
			DPrintf("[%d][%s][%d][ElectionTimer] give up: %d", rf.me, rf.role, rf.CurrentTerm, electionTimeout)
			rf.mu.Unlock()
			continue
		}

		if rf.lastHeatBeatTime.Equal(lastHeartBeatTime) {
			DPrintf("[%d][%s][%d][ElectionTimer] try convert to candidate, send ElectionTimeOutEvent(%d)", rf.me, rf.role, rf.CurrentTerm, electionTimeout)
			rf.RoleTransferStateMachine(ElectionTimeOutEvent)
		}
		rf.mu.Unlock()
	}
}

// A state machine of Raft Role driven by event.
// Whenever Raft transfer it's role, it should send an event to
// the channel, then the state machine will transfer it's state based on
// it's current state and the event it receive. It will also trigger the
// behaviour of each role
// don't guarantee concurrency safety, should be called with lock protection
func (rf *Raft) RoleTransferStateMachine(event EventType) {
	switch event {
	case ElectionTimeOutEvent:
		rf.checkRole([]RoleType{RoleCandidate, RoleFollower}, ElectionTimeOutEvent)
		rf.CandidateInit()
	case OutDatedEvent:
		rf.FollowerInit()
	case WinElectionEvent:
		rf.checkRole([]RoleType{RoleCandidate}, WinElectionEvent)
		rf.LeaderInit()
	case NewLeaderElectedEvent:
		rf.checkRole([]RoleType{RoleCandidate}, NewLeaderElectedEvent)
		rf.FollowerInit()
	}
}

func (rf *Raft) CandidateInit() {
	DPrintf("[%d][%s][%d][CandidateInit] become candidate", rf.me, rf.role, rf.CurrentTerm)
	rf.role = RoleCandidate
	rf.setVoteFor(rf.me)
	rf.lastHeatBeatTime = time.Now()
	rf.setCurrentTerm(rf.CurrentTerm + 1)

	rf.candidateVoteCount = 1

	// send request vote RPCs
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{}
		args.Term = rf.CurrentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = rf.Log[len(rf.Log) - 1].Index
		args.LastLogTerm = rf.Log[len(rf.Log) - 1].Term

		reply := RequestVoteReply{}

		go rf.sendRequestVote(i, &args, &reply)
	}
}

type RequestVoteArgs struct {
	Term, CandidateId int
	LastLogIndex, LastLogTerm int // Index and Term of candidate's last Log entry
}

type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

// should be called with a new go routine
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[%d][%s][%d][sendRequestVote] send request vote to %d", rf.me, rf.role, rf.CurrentTerm, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check this goroutine stale or not
	if rf.CurrentTerm > args.Term || rf.role != RoleCandidate {
		return
	}

	if rf.killed() {
		return
	}

	if reply.Term > rf.CurrentTerm {
		DPrintf("[%d][%s][%d][sendRequestVote] find itself a stale peer, try transfer to follower", rf.me, rf.role, rf.CurrentTerm)
		rf.RoleTransferStateMachine(OutDatedEvent)
		rf.setCurrentTerm(args.Term)
		return
	}

	if !ok {
		DPrintf("[%d][%s][%d][sendRequestVote] fail to receive vote from %d due to network issue", rf.me, rf.role, rf.CurrentTerm, server)
		// manual retry
		nArgs := RequestVoteArgs{}
		nArgs.CandidateId = args.CandidateId
		nArgs.Term = args.Term
		nArgs.LastLogIndex = len(rf.Log) - 1
		nArgs.LastLogTerm = rf.Log[nArgs.LastLogTerm].Term
		nReply := RequestVoteReply{}
		go rf.sendRequestVote(server, &nArgs, &nReply)
		return
	}

	if !reply.VoteGranted {
		DPrintf("[%d][%s][%d][sendRequestVote] vote rejected by %d", rf.me, rf.role, rf.CurrentTerm, server)
	}

	if reply.VoteGranted {
		DPrintf("[%d][%s][%d][sendRequestVote] received vote from %d", rf.me, rf.role, rf.CurrentTerm, server)
		rf.candidateVoteCount++
		// If votes received from majority of servers: try become leader
		DPrintf("[%d][%s][%d][sendRequestVote] received candidateVoteCount: %d",rf.me,
			rf.role, rf.CurrentTerm, rf.candidateVoteCount)
		if rf.candidateVoteCount * 2 > len(rf.peers) {
			DPrintf("[%d][%s][%d][sendRequestVote] received from majority of servers(%d): try become leader",
				rf.me, rf.role, rf.CurrentTerm, rf.candidateVoteCount)
			rf.RoleTransferStateMachine(WinElectionEvent)
		}
	}
}

func (rf *Raft) FollowerInit() {
	DPrintf("[%d][%s][%d][FollowerInit] become follower", rf.me, rf.role, rf.CurrentTerm)
	rf.role = RoleFollower
	rf.setVoteFor(-1)
}

// Respond to RPCs from candidates and leaders
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If the Term outdated, return false
	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		DPrintf("[%d][%s][%d][RequestVote] rej, args.Term < rf.CurrentTerm", rf.me, rf.role, rf.CurrentTerm)
		return
	}

	// If RPC request or response contains term T > CurrentTerm:
	//set CurrentTerm = T, convert to follower (§5.1)
	if args.Term > rf.CurrentTerm {
		rf.RoleTransferStateMachine(OutDatedEvent)
		rf.setCurrentTerm(args.Term)
	}

	// rf.VoteFor is null or vote for it self (state = candidate, vote for itself / follower)
	// and candidate’s Log is at least as up-to-date as receiver’s Log, grant vote
	selfLastLogIndex := rf.Log[len(rf.Log) - 1].Index
	selfLastLogTerm := rf.Log[len(rf.Log) - 1].Term
	DPrintf("[%d][%s][%d][RequestVote] selfLastLog: [%d, %d] candidateLastLog: [%d, %d], VoteFor: %d, args.Term: %d, rf.term: %d",
		rf.me, rf.role, rf.CurrentTerm, selfLastLogIndex, selfLastLogTerm, args.LastLogIndex, args.LastLogTerm, rf.VoteFor, args.Term, rf.CurrentTerm)
	if !(args.LastLogTerm > selfLastLogTerm ||
		(args.LastLogTerm == selfLastLogTerm && args.LastLogIndex >= selfLastLogIndex)) {
		DPrintf("[%d][%s][%d][RequestVote] failed log more up-to-date check", rf.me, rf.role, rf.CurrentTerm)
		return
	}

	// check If votedFor is null or candidateId
	if rf.VoteFor != -1 && rf.VoteFor != args.CandidateId {
		DPrintf("[%d][%s][%d][RequestVote] rej, can't satisfy 'If votedFor is null or candidateId'", rf.me, rf.role, rf.CurrentTerm)
		return
	}

	reply.VoteGranted = true
	rf.setVoteFor(args.CandidateId)
	DPrintf("[%d][%s][%d][RequestVote] %d vote for %d", rf.me, rf.role, rf.CurrentTerm, rf.me, rf.VoteFor)

	rf.lastHeatBeatTime = time.Now()
}

// Respond to AppendEntry RPCs from leaders
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < CurrentTerm (§5.1)
	reply.Term = rf.CurrentTerm
	reply.Success = false
	if args.Term < rf.CurrentTerm {
		DPrintf("[%d][%s][%d][AppendEntries] Reply false if term < CurrentTerm",
			rf.me, rf.role, rf.CurrentTerm)
		return
	}

	// If RPC request or response contains term T > CurrentTerm convert to follower (§5.1)
	if args.Term > rf.CurrentTerm {
		rf.RoleTransferStateMachine(OutDatedEvent)
		rf.setCurrentTerm(args.Term)
		// and follow the leader
		rf.setVoteFor(args.Term)
	}

	// If candidate get AppendEntries RPC received from new leader: convert to follower
	if rf.role == RoleCandidate {
		rf.RoleTransferStateMachine(NewLeaderElectedEvent)
		//// and follow the leader
		//rf.setVoteFor(args.LeaderId)
	}

	if rf.VoteFor != args.LeaderId {
		reply.FLastLogTerm = -1
		return
	}

	rf.lastHeatBeatTime = time.Now()

	//  Reply false if Log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.Log) || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d][%s][%d][AppendEntries] Reply false if Log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm",
			rf.me, rf.role, rf.CurrentTerm)
		DPrintf("[%d][%s][%d][AppendEntries] args.PrevLogIndex: %d, args.PrevLogTerm %d",
			rf.me, rf.role, rf.CurrentTerm, args.PrevLogIndex, args.PrevLogTerm)
		if args.PrevLogIndex >= len(rf.Log) {
			DPrintf("[%d][%s][%d][AppendEntries] args.PrevLogIndex >= len(rf.Log)",
				rf.me, rf.role, rf.CurrentTerm)
		}
		if args.PrevLogIndex < len(rf.Log) && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
			DPrintf("[%d][%s][%d][AppendEntries] rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm",
				rf.me, rf.role, rf.CurrentTerm)
		}
		// use binary search to find the biggest index which term < args.PrevLogTerm
		l, r := 0, len(rf.Log) - 1
		for l < r {
			mid := (l + r + 1) >> 1
			if rf.Log[mid].Term < args.PrevLogTerm {
				l = mid
			} else {
				r = mid - 1
			}
		}
		reply.FLastLogIndex = l
		reply.FLastLogTerm = rf.Log[l].Term
		DPrintf("[%d][%s][%d][AppendEntries] reply FLastLogIndex: %d, FLastLogTerm: %d",
			rf.me, rf.role, rf.CurrentTerm, l, rf.Log[l].Term)
		return
	}

	// reply success
	reply.Success = true

	// update logs
	for _, v := range args.Entries {
		if v.Index < len(rf.Log) {
			// If an existing entry conflicts with a new one (same index
			// but different terms), delete the existing entry and all that
			// follow it (§5.3)
			if rf.Log[v.Index] != v {
				rf.Log[v.Index] = v
				rf.Log = rf.Log[:v.Index + 1]
			}
		} else {
			// Append any new entries not already in the Log
			rf.Log = append(rf.Log, v)
		}
	}
	rf.persist()

	DPrintf("[%d][%s][%d][AppendEntries] reply success, Log: %s", rf.me, rf.role, rf.CurrentTerm, serialize(rf.Log))

	// update commit
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.Log) - 1)
		DPrintf("[%d][%s][%d][AppendEntries] update commitIndex: %d, term %d", rf.me, rf.role, rf.CurrentTerm, rf.commitIndex,
		rf.Log[rf.commitIndex].Term)
		rf.cond.Signal()
	}

}

func serializeInt(arr []int) string {
	str := ""
	for _, v := range arr {
		str += strconv.Itoa(v)
		str += ", "
	}
	return str
}

func serialize(log []LogEntry) string {
	str := ""
	for _, v := range log {
		str += "["
		str += strconv.Itoa(v.Index)
		str += ", "
		str += strconv.Itoa(v.Term)
		str += "], "
	}
	return str
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) LeaderInit() {
	DPrintf("[%d][%s][%d][LeaderInit] become leader", rf.me, rf.role, rf.CurrentTerm)
	rf.role = RoleLeader

	// initialized to leader last Log index + 1
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.Log)
	}
	// initialized to 0
	for i, _ := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}

	// send heart beat
	go rf.HeartBeat(rf.CurrentTerm)
}

// should be called with a new go routine
func (rf *Raft) HeartBeat(term int) {
	DPrintf("[%d][%s][%d][HeartBeat] start heart beating", rf.me, rf.role, rf.CurrentTerm)
	for {
		str := ""
		rf.mu.Lock()
		for i, _ := range rf.nextIndex {
			str += "[n:"
			str += strconv.Itoa(rf.nextIndex[i])
			str += ", m:"
			str += strconv.Itoa(rf.matchIndex[i])
			str += "], "
		}
		DPrintf("[%d][%s][%d][HeartBeat] send heart beat %s", rf.me, rf.role, rf.CurrentTerm, str)

		for i, _ := range rf.peers {

			// check this goroutine stale or not
			if rf.CurrentTerm > term || rf.role != RoleLeader {
				rf.mu.Unlock()
				return
			}

			if i == rf.me {
				rf.lastHeatBeatTime = time.Now()
				continue
			}

			if rf.killed() {
				rf.mu.Unlock()
				return
			}

			args := AppendEntriesArgs{}
			args.Term = rf.CurrentTerm
			args.LeaderId = rf.me
			args.LeaderCommit = rf.commitIndex
			reply := AppendEntriesReply{}

			go rf.sendAppendEntries(i, &args, &reply)
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(HeatBeatRate))
	}

}

type AppendEntriesArgs struct {
	Term, LeaderId int
	PrevLogIndex, PrevLogTerm int // Index and Term of Log entry immediately preceding new ones
	Entries []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term int
	Success bool
	FLastLogIndex, FLastLogTerm int // Index and Term of follower's last Log entry
}

// should be called with a new go routine
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
	args.Entries = rf.Log[rf.nextIndex[server]:]
	rf.mu.Unlock()

	DPrintf("[%d][%s][%d][sendAppendEntries] send append entry to %d, args.PrevLogIndex: %d, args.PrevLogTerm: %d",
		rf.me, rf.role, rf.CurrentTerm, server, args.PrevLogIndex, args.PrevLogTerm)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)


	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check this goroutine stale or not
	if rf.CurrentTerm > args.Term || rf.role != RoleLeader ||
		args.PrevLogIndex != rf.nextIndex[server] - 1 ||
		args.PrevLogTerm != rf.Log[args.PrevLogIndex].Term {
		return
	}

	if rf.killed() {
		return
	}

	if reply.Term > rf.CurrentTerm {
		DPrintf("[%d][%s][%d][sendAppendEntries] find itself a stale peer, try transfer to follower", rf.me, rf.role, rf.CurrentTerm)
		rf.RoleTransferStateMachine(OutDatedEvent)
		rf.setCurrentTerm(args.Term)
		return
	}

	if !ok {
		DPrintf("[%d][%s][%d][sendAppendEntries] fail to send entry to %d due to network issue", rf.me, rf.role, rf.CurrentTerm, server)
		// manual retry
		nArgs := AppendEntriesArgs{}
		nReply := AppendEntriesReply{}
		go rf.sendAppendEntries(server, &nArgs, &nReply)
		return
	}

	if !reply.Success {
		DPrintf("[%d][%s][%d][sendAppendEntries] rejected by %d", rf.me, rf.role, rf.CurrentTerm, server)

		if rf.nextIndex[server] < 1 {
			FPrintf("[%d][%s][%d][sendAppendEntries] next index go down to 0", rf.me, rf.role, rf.CurrentTerm)
		}

		if rf.matchIndex[server] == len(rf.Log) {
			return
		}

		if reply.FLastLogTerm == -1 {
			return
		}

		// can find the pre index in leader's log
		if reply.FLastLogIndex < len(rf.Log) && rf.Log[reply.FLastLogIndex].Term == reply.FLastLogTerm {
			rf.nextIndex[server] = reply.FLastLogIndex + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			//rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		} else {
			// use binary search to find the biggest index which term < reply.FLastLogTerm
			l, r := 0, len(rf.Log) - 1
			for l < r {
				mid := (l + r + 1) >> 1
				if rf.Log[mid].Term < reply.FLastLogTerm {
					l = mid
				} else {
					r = mid - 1
				}
			}
			rf.nextIndex[server] = l + 1
		}
		DPrintf("[%d][%s][%d][sendAppendEntries] update %d's nextIndex: %d", rf.me, rf.role, rf.CurrentTerm, server, rf.nextIndex[server])

		nArgs := AppendEntriesArgs{}

		nReply := AppendEntriesReply{}

		go rf.sendAppendEntries(server, &nArgs, &nReply)
	}

	if reply.Success {

		if len(args.Entries) >= 1 {
			DPrintf("[%d][%s][%d][sendAppendEntries] update %d's matchIndex: %d", rf.me, rf.role, rf.CurrentTerm, server, args.Entries[len(args.Entries) - 1].Index)
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			// counting replication
			newCommitIndex := rf.commitIndex
			for {
				ct := 0
				for _, v := range rf.matchIndex {
					if v >= newCommitIndex + 1 {
						ct++
					}
				}
				if ct * 2 > len(rf.peers){
					// respond after entry applied to state machine (§5.3)
					newCommitIndex++
				} else {
					break
				}
			}

			// a leader cannot determine commitment using log entries from older terms
			if rf.Log[newCommitIndex].Term < rf.CurrentTerm {
				return
			}

			rf.commitIndex = newCommitIndex
			DPrintf("[%d][%s][%d][sendAppendEntries] update commitIndex: %d, term %d, commitIndex: %s",
				rf.me, rf.role, rf.CurrentTerm, rf.commitIndex, rf.Log[rf.commitIndex].Term, serializeInt(rf.matchIndex))
			rf.cond.Signal()
		}
	}

}

func (rf *Raft) setCurrentTerm(newTerm int) {
	rf.CurrentTerm = newTerm
	rf.persist()
}

func (rf *Raft) setVoteFor(newVoteFor int) {
	rf.VoteFor = newVoteFor
	rf.persist()
}

func (rf *Raft) checkRole(expectRoleType[] RoleType, eventType EventType) {
	intact := true
	for _, role := range expectRoleType {
		if role == rf.role {
			intact = false
		}
	}
	if intact {
		FPrintf("raise event %s with role %s", eventType, rf.role)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.role == RoleLeader

	if !isLeader {
		return index, term, isLeader
	}

	// is leader
	index = len(rf.Log)
	term = rf.CurrentTerm
	DPrintf("[%d][%s][%d][Start] new Log with index: %d term: %d cmd: %v", rf.me, rf.role, rf.CurrentTerm, index, term, command)

	// If Command received from client: append entry to local Log,
	//respond after entry applied to state machine (§5.3)
	entry := LogEntry{
		Command: command,
		Term:    term,
		Index:   index,
	}

	rf.Log = append(rf.Log, entry)
	rf.persist()

	rf.matchIndex[rf.me] = len(rf.Log) - 1
	rf.nextIndex[rf.me] = len(rf.Log)

	for i, _ := range rf.peers {
		if i == rf.me {
			rf.lastHeatBeatTime = time.Now()
			continue
		}

		args := AppendEntriesArgs{}
		args.Term = rf.CurrentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		reply := AppendEntriesReply{}

		go rf.sendAppendEntries(i, &args, &reply)
	}

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
	defer rf.mu.Unlock()
	DPrintf("[%d][%s][%d][Kill] killed", rf.me, rf.role, rf.CurrentTerm)
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
	rf.VoteFor = -1
	rf.CurrentTerm = 0
	rf.lastHeatBeatTime = time.Now()
	rf.role = RoleFollower
	rf.candidateVoteCount = 0

	// append an default entry to avoid boundary error
	rf.Log = []LogEntry{}
	rf.Log = append(rf.Log, LogEntry{
		Term:  0,
		Index: 0,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.cond = sync.NewCond(new(sync.Mutex))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("[%d][%s][%d][Make] start", rf.me, rf.role, rf.CurrentTerm)
	go rf.ElectionTimer()
	go rf.applyMsg()

	return rf
}

func (rf *Raft) applyMsg() {
	for !rf.killed() {
		rf.cond.L.Lock()
		rf.cond.Wait()
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			DPrintf("[%d][%s][%d][applyMsg] index %d applied!, term %d, cmd %v", rf.me, rf.role, rf.CurrentTerm, i, rf.Log[i].Term, rf.Log[i].Command)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i].Command,
				CommandIndex: i,
			}
			rf.applyCh <- msg
		}
		rf.lastApplied = rf.commitIndex
		rf.cond.L.Unlock()
	}
}

func arrEqual(arr1, arr2 []LogEntry) bool {
	if len(arr1) != len(arr2) {
		return false
	}
	for i, _ := range arr1 {
		if arr1[i] != arr2[i] {
			return false
		}
	}
	return true
}

