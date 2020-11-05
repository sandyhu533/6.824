# 6.824
 Implementation for MIT [6.824](http://nil.csail.mit.edu/6.824/2020/schedule.html) Distributed System - 2020

## lab1 : build a MapReduce system
## lab2 : Raft
* Implement Raft leader election and heartbeats.
    * Some bugs I met:
    * Forget to kill the thread when peer is kill, and the thread continue to run in the next test(2A-2).
    * Corner cases
        * In the 'Split Brain' case, the stale leader should turn to follower when receiving the heartbeat of the new leader.
        * During a sleep of an RPC, the peer state may change. For Example, a candidate turn to follower, then the reply of vote rpc shouldn't be count.
    * Update the field. Forget to update the field 'voteFor' when turned to leader.
    * Be careful to infinite loop. Avoid it unless required by function.