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
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		DPrintf("[%d][RequestVote] rej, args.Term < rf.CurrentTerm", rf.me)
		return
	}

	// If RPC request or response contains term T > CurrentTerm:
	//set CurrentTerm = T, convert to follower (§5.1)
	if args.Term > rf.CurrentTerm {
		rf.tryConvertToFollower(rf.me, rf.CurrentTerm, args.Term)
	}

	// If votedFor is null or candidateId
	if rf.VoteFor != -1 && rf.VoteFor != args.CandidateId {
		DPrintf("[%d][RequestVote] rej, can't satisfy 'If votedFor is null or candidateId'", rf.me)
		return
	}

	// rf.VoteFor is null or vote for it self (state = candidate, vote for itself / follower)
	// and candidate’s Log is at least as up-to-date as receiver’s Log, grant vote
	selfLastLogIndex := rf.Log[len(rf.Log) - 1].Index
	selfLastLogTerm := rf.Log[len(rf.Log) - 1].Term
	DPrintf("[%d][RequestVote] selfLastLog: [%d, %d] candidateLastLog: [%d, %d], VoteFor: %d, args.Term: %d, rf.term: %d",
		rf.me, selfLastLogIndex, selfLastLogTerm, args.LastLogIndex, args.LastLogTerm, rf.VoteFor, args.Term, rf.CurrentTerm)
	if (rf.VoteFor == -1 || rf.VoteFor == args.CandidateId) &&
		(args.LastLogTerm > selfLastLogTerm ||
			(args.LastLogTerm == selfLastLogTerm && args.LastLogIndex >= selfLastLogIndex)) {
		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId
		rf.persist()
		DPrintf("[%d][RequestVote] %d vote for %d", rf.me, rf.me, rf.VoteFor)

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

	// Reply false if term < CurrentTerm (§5.1)
	reply.Term = rf.CurrentTerm
	reply.Success = false
	if args.Term < rf.CurrentTerm {
		DPrintf("[%d][AppendEntries] Reply false if term < CurrentTerm",
			rf.me)
		return
	}

	// If RPC request or response contains term T > CurrentTerm:
	//set CurrentTerm = T, convert to follower (§5.1)
	if args.Term > rf.CurrentTerm {
		rf.tryConvertToFollower(rf.me, rf.CurrentTerm, args.Term)
	}

	if !(rf.role == RoleFollower || (rf.role == RoleLeader && rf.me == args.LeaderId)) {
		return
	}

	//  Reply false if Log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= len(rf.Log) || rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d][AppendEntries] Reply false if Log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm",
			rf.me)
		return
	}

	// reply success
	reply.Success = true

	rf.lastHeatBeatTime = time.Now()

	// update logs
	for _, v := range args.Entries {
		if v.Index < len(rf.Log) {
			// If an existing entry conflicts with a new one (same index
			//but different terms), delete the existing entry and all that
			//follow it (§5.3)
			if rf.Log[v.Index] != v {
				rf.Log[v.Index] = v
				rf.Log = rf.Log[:v.Index + 1]
			}
		} else {
			// Append any new entries not already in the Log
			rf.Log = append(rf.Log, v)
		}
		rf.persist()
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
	for _, v := range rf.Log {
		str += "["
		str += strconv.Itoa(v.Index)
		str += ", "
		str += strconv.Itoa(v.Term)
		str += "], "
	}

	DPrintf("[%d][AppendEntries] reply success, Log: %s", rf.me, str)

	// update commit
	if args.LeaderCommit > rf.commitIndex {
		originCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.Log) - 1)
		DPrintf("[%d][AppendEntries] originCommitIndex: %d  newCommitIndex: %d", rf.me, originCommitIndex, rf.commitIndex)
		for i := originCommitIndex + 1; i <= rf.commitIndex; i++ {
			DPrintf("[%d][AppendEntries] %d committed!, term %d, cmd %v", rf.me, i, rf.Log[i].Term, rf.Log[i].Command)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[i].Command,
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