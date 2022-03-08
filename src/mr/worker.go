package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.

	for {
		reply := RequestTask()
		if reply.TaskType == 0 {

		} else if reply.TaskType == 1 {
			DoMap(reply, mapf, reducef)
		} else {
			DoReduce(reply, mapf, reducef)
		}

		time.Sleep(time.Second)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoReduce(reply *TaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	pattern := "mr-\\d+-" + strconv.Itoa(reply.ReduceTask.ReduceId)
	var files []string
	reg, _ := regexp.Compile(pattern)
	root, _ := os.Getwd()
	filepath.Walk(root, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			fmt.Println(err)
			return err
		}

		if f.IsDir() {
			return nil
		}

		// match
		matched := reg.MatchString(f.Name())

		if matched {
			files = append(files, f.Name())
		}
		return nil
	})

	intermediate := []KeyValue{}
	for _, file := range files {
		ofile, _ := os.Open(file)
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ofile.Close()
	}

	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.ReduceTask.ReduceId)
	ofile, _ := os.Create(oname)
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
	ofile.Close()
	var res string
	call("Master.TaskDone", reply, &res)
}

func DoMap(reply *TaskReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	intermediate := [10][]KeyValue{}
	for _, filename := range reply.MapTask.Mapfiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, v := range kva {
			KeyId := ihash(v.Key) % (reply.nReduce)
			intermediate[KeyId] = append(intermediate[KeyId], v)
		}
	}
	for i := 0; i < reply.nReduce; i++ {
		oname := "mr-" + strconv.Itoa(reply.MapTask.MapId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write %v", oname)
			}
		}
		ofile.Close()
	}
	var res string
	call("Master.TaskDone", reply, &res)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func RequestTask() *TaskReply {

	// declare an argument structure.
	args := RequestArgs{}
	// fill in the argument(s).
	// declare a reply structure.
	reply := TaskReply{nReduce: 10}
	// send the RPC request, wait for the reply.
	call("Master.GetTask", &args, &reply)
	return &reply
}

//
// send an RPC request to the coordinator, wait for the response.
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
