package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


type DoneMapArgs struct {
	Index int
}

type DoneMapReply struct {
}

type DoneReduceArgs struct {
	Index int
}

type DoneReduceReply struct {
}

type WorkArgs struct {
}


type WorkReply struct {
	Flag, Index   int // Flag 1: map, 2: reduce, 0: no remain work
	MMap, NReduce int
	MapReplyFile  string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
