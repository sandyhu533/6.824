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

	// If peer itself is leader, and the candidate's term less then self's, return false
	if rf.role == RoleLeader && rf.currentTerm == args.Term {
		DPrintf("[%d][RequestVote] rej, If peer itself is leader, and the candidate's term less then self's", rf.me)
		return
	}

	// rf.voteFor is null or vote for it self (state = candidate, vote for itself / follower)
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	selfLastLogIndex := rf.log[len(rf.log) - 1].Index
	selfLastLogTerm := rf.log[len(rf.log) - 1].Term
	DPrintf("[%d][RequestVote] selfLastLog: [%d, %d] candidateLastLog: [%d, %d], voteFor: %d",
		rf.me, selfLastLogIndex, selfLastLogTerm, args.LastLogIndex, args.LastLogTerm, rf.voteFor)
	if (args.Term > rf.currentTerm || rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&
		((args.LastLogTerm > selfLastLogTerm) ||
			(args.LastLogTerm == selfLastLogTerm && args.LastLogIndex >= selfLastLogIndex)) {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
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
		//DPrintf("[%d][AppendEntries] Reply false if term < currentTerm",
		//	rf.me)
		return
	}

	//  Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogTerm >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		//DPrintf("[%d][AppendEntries] Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm",
		//	rf.me)
		return
	}

	// reply success
	reply.Success = true
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	// If AppendEntries RPC received from new leader: convert to
	// follower
	if args.LeaderId != rf.me {
		rf.role = RoleFollower
	}

	rf.lastHeatBeatTime = time.Now()

	// update commit
	if args.LeaderCommit > rf.commitIndex {
		originCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
		if rf.commitIndex == originCommitIndex + 1 {
			DPrintf("[%d][AppendEntries] %d committed!, term %d", rf.me, rf.commitIndex, rf.log[rf.commitIndex].Term)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.commitIndex].Command,
				CommandIndex: rf.commitIndex,
			}
			rf.applyCh <- msg
		}
	}

	// heartbeats
	if args.Entries == nil {
		return
	}

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

	DPrintf("[%d][AppendEntries] reply success, entries: %s", rf.me, str)

	str = ""
	for _, v := range rf.log {
		str += "["
		str += strconv.Itoa(v.Index)
		str += ", "
		str += strconv.Itoa(v.Term)
		str += "], "
	}

	DPrintf("[%d][AppendEntries] reply success, log: %s", rf.me, str)

}


func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}