package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type mapUnit struct {
	done, divided bool
	time          time.Time
}

type reduceUnit struct {
	done, divided bool
	time          time.Time
}

type Master struct {
	mMap, nReduce int
	files []string

	remainMapCount    int
	remainReduceCount int
	maps			  []mapUnit
	reduces			  []reduceUnit
}

var givingMapMutex sync.Mutex
var remainMapMutex sync.Mutex
var remainReduceMutex sync.Mutex

//
// The RPC function for workers to pick work, will return a map or reduce work
//

func (m *Master) PickWork(args *WorkArgs, reply *WorkReply) error {

	// all the work of map and reduce done
	remainReduceMutex.Lock()
	if m.remainReduceCount == 0 {
		reply.Flag = 0
		remainReduceMutex.Unlock()
	} else {
		reply.NReduce = m.nReduce
		reply.MMap = m.mMap

		remainMapMutex.Lock()
		if m.remainMapCount != 0 {
			// do map
			for i, v := range m.maps {
				if !m.maps[i].done && (!m.maps[i].divided || time.Now().Sub(v.time).Seconds() > 10.0) {
					log.Print("[MASTER] do map")
					reply.Flag = 1
					reply.Index = i
					reply.MapReplyFile = m.files[i]
					m.maps[i].time = time.Now()
					m.maps[i].divided = true
					break
				}
			}
			remainMapMutex.Unlock()
			remainReduceMutex.Unlock()
		} else {
			// do reduce
			remainMapMutex.Unlock()

			for m.remainMapCount != 0 {
			}

			// do reduce
			for i, v := range m.reduces {
				if !m.reduces[i].done && (!m.reduces[i].divided || time.Now().Sub(v.time).Seconds() > 10.0) {
					log.Print("[MASTER] do reduce")
					reply.Flag = 2
					reply.Index = i
					m.reduces[i].time = time.Now()
					m.reduces[i].divided = true
					break
				}
			}
			remainReduceMutex.Unlock()
		}
	}

	return nil
}

//
// The RPC function to announce master the map work is done
//
func (m *Master) DoneMap(args *DoneMapArgs, reply *DoneMapReply) error {
	remainMapMutex.Lock()
	m.remainMapCount--
	m.maps[args.Index].done = true
	log.Print("[MASTER] after map, m.remainMapCount: " + strconv.Itoa(m.remainMapCount))
	remainMapMutex.Unlock()
	return nil
}

//
// The RPC function to announce master the reduce work is done
//
func (m *Master) DoneReduce(args *DoneReduceArgs, reply *DoneReduceReply) error {
	remainReduceMutex.Lock()
	m.remainReduceCount--
	m.reduces[args.Index].done = true
	log.Print("[MASTER] after reduce, m.remainReduceCount: " + strconv.Itoa(m.remainReduceCount))
	remainReduceMutex.Unlock()
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.remainReduceCount == 0 {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mMap = len(files)
	m.nReduce = nReduce
	m.files = files

	m.remainReduceCount = nReduce
	m.remainMapCount = m.mMap
	m.maps = make([]mapUnit, m.mMap)
	m.reduces = make([]reduceUnit, nReduce)

	m.server()
	return &m
}
