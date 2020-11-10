package raft

import (
	"strconv"
	"time"
)

// Respond to RPCs from candidates and leaders
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If the Term outdated, return false
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[%d][RequestVote] rej, args.Term < rf.currentTerm", rf.me)
		return
	}

	// If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.tryConvertToFollower(rf.me, rf.currentTerm, args.Term)
	}

	// If votedFor is null or candidateId
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		DPrintf("[%d][RequestVote] rej, can't satisfy 'If votedFor is null or candidateId'", rf.me)
		return
	}

	// rf.voteFor is null or vote for it self (state = candidate, vote for itself / follower)
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	selfLastLogIndex := rf.log[len(rf.log) - 1].Index
	selfLastLogTerm := rf.log[len(rf.log) - 1].Term
	DPrintf("[%d][RequestVote] selfLastLog: [%d, %d] candidateLastLog: [%d, %d], voteFor: %d, args.Term: %d, rf.term: %d",
		rf.me, selfLastLogIndex, selfLastLogTerm, args.LastLogIndex, args.LastLogTerm, rf.voteFor, args.Term, rf.currentTerm)
	if (args.Term > rf.currentTerm ||  rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		(args.LastLogTerm > selfLastLogTerm ||
			(args.LastLogTerm == selfLastLogTerm && args.LastLogIndex >= selfLastLogIndex)) {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.currentTerm = args.Term
		DPrintf("[%d][RequestVote] %d vote for %d", rf.me, rf.me, rf.voteFor)

		if args.CandidateId != rf.me {
			rf.role = RoleFollower
		}
	}

}

// Respond to RPCs from candidates and leaders
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm (§5.1)
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		DPrintf("[%d][AppendEntries] Reply false if term < currentTerm",
			rf.me)
		return
	}

	// If RPC request or response contains term T > currentTerm:
	//set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.tryConvertToFollower(rf.me, rf.currentTerm, args.Term)
	}

	if !(rf.role == RoleFollower || (rf.role == RoleLeader && rf.me == args.LeaderId)) {
		return
	}

	//  Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d][AppendEntries] Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm",
			rf.me)
		return
	}

	// reply success
	reply.Success = true
	rf.currentTerm = args.Term

	rf.lastHeatBeatTime = time.Now()

	// update logs
	for _, v := range args.Entries {
		if v.Index < len(rf.log) {
			// If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that
			//follow it (§5.3)
			if rf.log[v.Index] != v {
				rf.log[v.Index] = v
				rf.log = rf.log[:v.Index + 1]
			}
		} else {
			// Append any new entries not already in the log
			rf.log = append(rf.log, v)
		}
	}

	str := ""
	for _, v := range args.Entries {
		str += "["
		str += strconv.Itoa(v.Index)
		str += ", "
		str += strconv.Itoa(v.Term)
		str += "], "
	}

	// heartbeats
	if args.Entries != nil {
		DPrintf("[%d][AppendEntries] reply success, entries: %s", rf.me, str)
	}


	str = ""
	for _, v := range rf.log {
		str += "["
		str += strconv.Itoa(v.Index)
		str += ", "
		str += strconv.Itoa(v.Term)
		str += "], "
	}

	DPrintf("[%d][AppendEntries] reply success, log: %s", rf.me, str)

	// update commit
	if args.LeaderCommit > rf.commitIndex {
		originCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
		DPrintf("[%d][AppendEntries] originCommitIndex: %d  newCommitIndex: %d", rf.me, originCommitIndex, rf.commitIndex)
		for i := originCommitIndex + 1; i <= rf.commitIndex; i++ {
			DPrintf("[%d][AppendEntries] %d committed!, term %d, cmd %v", rf.me, i, rf.log[i].Term, rf.log[i].Command)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.applyCh <- msg
		}
	}

}


func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}