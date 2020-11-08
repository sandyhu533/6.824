package raft

import (
	"errors"
	"strconv"
	"time"
)

type AppendEntriesArgs struct {
	Term, LeaderId int
	PrevLogIndex, PrevLogTerm int // Index and Term of log entry immediately preceding new ones
	Entries []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) leaderInit(me int, term int) {
	rf.role = RoleLeader
	rf.voteFor = -1
	rf.lastHeatBeatTime = time.Now()
	// for each server, index of the next log entry
	// to send to that server (initialized to leader
	// last log index + 1)
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	// for each server, index of highest log entry
	//known to be replicated on server
	//(initialized to 0, increases monotonically)
	for i, _ := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	go rf.sendHeartBeat(me, term)
}

//func (rf *Raft) sendNewLogToClient(me int, term int) {
//	for i, _ := range rf.peers {
//		if i == rf.me {
//			continue
//		}
//		if len(rf.log) > rf.nextIndex[i] {
//			args := AppendEntriesArgs{}
//			args.Term = term
//			args.LeaderId = me
//			args.PrevLogIndex = rf.nextIndex[i] - 1
//			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
//			args.Entries = rf.log[rf.nextIndex[i]:]
//			args.LeaderCommit = rf.commitIndex
//
//			reply := AppendEntriesReply{}
//
//			go rf.sendAppendEntries(me, i, term, &args, &reply)
//
//			rf.mu.Lock()
//
//			// If AppendEntries RPC received from new leader: convert to follower
//			if rf.role != RoleLeader || term < rf.currentTerm {
//				rf.mu.Unlock()
//				return
//			}
//
//			if rf.killed() {
//				return
//			}
//
//			rf.mu.Unlock()
//		}
//	}
//}

func (rf *Raft) sendAppendEntries(me int, i int, term int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//DPrintf("[%d][sendAppendEntries] send append entry to %d", me, i)

	ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)

	if !ok {
		DPrintf("[%d][sendAppendEntries] send to %d not ok", me, i)
	} else if !reply.Success {
		DPrintf("[%d][sendAppendEntries] fail to append entry to %d", me, i)
		if reply.Term > term {
			rf.tryConvertToFollower(me, term, reply.Term)
		} else {
			if rf.nextIndex[i] <= 1 {
				errors.New("next index go down to 0")
				return ok
			}
			rf.mu.Lock()

			DPrintf("[%d][sendAppendEntries] update %d's nextIndex: %d", rf.me, i, rf.nextIndex[i])
			rf.nextIndex[i]--
			args := AppendEntriesArgs{}
			args.Term = term
			args.LeaderId = me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[rf.nextIndex[i]:]
			args.LeaderCommit = rf.commitIndex

			reply := AppendEntriesReply{}

			go rf.sendAppendEntries(me, i, term, &args, &reply)

			rf.mu.Unlock()
		}
	} else {
		//DPrintf("[%d][sendAppendEntries] append entry to %d", me, i)
		if len(args.Entries) >= 1 {
			rf.mu.Lock()
			DPrintf("[%d][sendAppendEntries] update %d's matchIndex: %d", rf.me, i, args.Entries[len(args.Entries) - 1].Index)
			rf.nextIndex[i] = args.Entries[len(args.Entries) - 1].Index + 1
			rf.matchIndex[i] = args.Entries[len(args.Entries) - 1].Index
			// update commitIndex
			for {
				ct := 0
				for _, v := range rf.matchIndex {
					if v >= rf.commitIndex + 1 {
						ct++
					}
				}
				if ct * 2 > len(rf.peers){
					// respond after entry applied to state machine (§5.3)
					rf.commitIndex++
					DPrintf("[%d][sendAppendEntries] %d committed!, term %d", rf.me, rf.commitIndex, rf.log[rf.commitIndex].Term)
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[rf.commitIndex].Command,
						CommandIndex: rf.commitIndex,
					}
					rf.applyCh <- msg
				} else {
					break
				}
			}
			rf.mu.Unlock()
		}
	}

	return ok
}


// Upon election: send initial empty AppendEntries RPCs
//(heartbeat) to each server; repeat during idle periods to
//prevent election timeouts (§5.2)
func (rf *Raft) sendHeartBeat(me int, term int) {
	DPrintf("[%d][sendHeartBeat] do leader", me)
	for {
		str := ""
		for i, _ := range rf.nextIndex {
			rf.mu.Lock()
			str += "[n:"
			str += strconv.Itoa(rf.nextIndex[i])
			str += ", m:"
			str += strconv.Itoa(rf.matchIndex[i])
			str += "], "
			rf.mu.Unlock()
		}
		//DPrintf("[%d][sendHeartBeat] send heart beat %s", me, str)
		for i, _ := range rf.peers {
			args := AppendEntriesArgs{}
			args.Term = term
			args.LeaderId = me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = rf.log[rf.nextIndex[i]:]
			args.LeaderCommit = rf.commitIndex

			reply := AppendEntriesReply{}

			go rf.sendAppendEntries(me, i, term, &args, &reply)

			rf.mu.Lock()

			// If AppendEntries RPC received from new leader: convert to follower
			if rf.role != RoleLeader || term < rf.currentTerm {
				rf.mu.Unlock()
				return
			}

			if rf.killed() {
				return
			}

			rf.mu.Unlock()
		}
		time.Sleep(time.Millisecond * time.Duration(HeatBeatRate))
	}

}