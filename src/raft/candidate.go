package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term, CandidateId int
	LastLogIndex, LastLogTerm int // Index and Term of candidate's last Log entry
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
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
func (rf *Raft) sendRequestVote(me int, toPeer int, term int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("[%d][sendRequestVote] send request vote to %d", me, toPeer)
	ok := rf.peers[toPeer].Call("Raft.RequestVote", args, reply)
	// If RPC request or response contains Term T > CurrentTerm:
	// set CurrentTerm = T, convert to follower (§5.1)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || !reply.VoteGranted {
		DPrintf("[%d][sendRequestVote] fail to receive vote from %d", me, toPeer)
		rf.tryConvertToFollower(me, term, reply.Term)
	} else {
		DPrintf("[%d][sendRequestVote] received vote from %d", me, toPeer)

		// the peer's state changed during the RPC
		if rf.CurrentTerm > term || rf.role != RoleCandidate {
			return false
		}
		rf.candidateVoteCount++
		// If votes received from majority of servers: become leader
		if rf.candidateVoteCount * 2 > len(rf.peers) {
			DPrintf("[%d][gatherVotes] received from majority of servers(%d): become leader", me, rf.candidateVoteCount)
			rf.leaderInit(me, term)
		}
	}
	return ok
}

// Send RequestVote RPCs to all other servers
func (rf *Raft) gatherVotes(me int, term int) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d][gatherVotes] do candidate", me)
	rf.candidateVoteCount = 0

	for i, _ := range rf.peers {
		args := RequestVoteArgs{}
		args.Term = term
		args.CandidateId = me
		args.LastLogIndex = rf.Log[len(rf.Log) - 1].Index
		args.LastLogTerm = rf.Log[len(rf.Log) - 1].Term

		reply := RequestVoteReply{}

		go rf.sendRequestVote(me, i, term, &args, &reply)
	}

}