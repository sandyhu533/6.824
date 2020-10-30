package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"plugin"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		// Your worker implementation here.

		// uncomment to send the PickWork RPC to the master.
		workReply := PickWorkCall()

		if workReply.Flag == 1 {
			log.Print("[WORKER] Do map work")
			MapWorker(mapf, workReply)
			log.Print("[WORKER] Send Map Done to master")
			mapDoneCall(workReply.Index)
		} else if workReply.Flag == 2 {
			log.Print("[WORKER] Do reduce work")
			ReduceWorker(reducef, workReply)
			log.Print("[WORKER] Send Reduce Done to master")
			reduceDoneCall(workReply.Index)
		}

	}

}

func MapWorker(mapf func(string, string) []KeyValue, workReply WorkReply) {
	// read input file,
	log.Print("[WORKER] Read input file")
	intermediate := []KeyValue{}
	filename := workReply.MapReplyFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// pass it to Map, accumulate the intermediate Map output.
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// divide intermediate into N group
	nReduce := workReply.NReduce
	intermediates := make([][]KeyValue, nReduce)
	for _, v := range intermediate {
		intermediates[ihash(v.Key) % nReduce] = append(intermediates[ihash(v.Key) % nReduce], v)
	}

	// write intermediate into template file
	var fileNames []string
	for i := 0; i < nReduce; i++ {
		// create intermediate file
		oname := "mr-" + strconv.Itoa(workReply.Index) + "-" + strconv.Itoa(i)
		log.Print("[WORKER] oname:" + oname)
		ofile, _ := ioutil.TempFile("", oname + "*")

		// write to json file
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediates[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("[WORKER] Encode failer")
			}
		}

		fileNames = append(fileNames, ofile.Name())
	}

	// rename template file
	for i, v := range fileNames {
		oname := "mr-" + strconv.Itoa(workReply.Index) + "-" + strconv.Itoa(i)
		os.Rename(v, oname)
	}
}

func ReduceWorker(reducef func(string, []string) string, workReply WorkReply) {

	// read intermediate file
	mMap := workReply.MMap
	intermediate := []KeyValue{}
	for i := 0; i < mMap; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(workReply.Index)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		// read from json file
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-X.
	//

	oname := "mr-out-" + strconv.Itoa(workReply.Index)

	// create template file
	ofile, _ := ioutil.TempFile("", oname + "*")

	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// rename template file
	os.Rename(ofile.Name(), oname)
}

//
// function to show how to make an RPC call to the master, to pick work
//
func PickWorkCall() WorkReply {

	// declare an argument structure.
	args := WorkArgs{}

	// declare a reply structure.
	reply := WorkReply{0,0,0,0,""}

	// send the RPC request, wait for the reply.
	if !call("Master.PickWork", &args, &reply) {
		os.Exit(1)
	}


	return reply
}

//
// make an RPC call to the master informing map done
//
func mapDoneCall(index int) {

	// declare an argument structure.
	args := DoneMapArgs{}
	args.Index = index

	// declare a reply structure.
	reply := DoneMapReply{}

	// send the RPC request, wait for the reply.
	call("Master.DoneMap", &args, &reply)
}

//
// make an RPC call to the master informing reduce done
//
func reduceDoneCall(index int) {

	// declare an argument structure.
	args := DoneMapArgs{}
	args.Index = index

	// declare a reply structure.
	reply := DoneMapReply{}

	// send the RPC request, wait for the reply.
	call("Master.DoneReduce", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

