package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// State  0: undo, 1: doing, 2: done
type MapTask struct {
	Mapfiles  []string
	MapId     int
	StartTime int64
	State     int
}

type ReduceTask struct {
	Reducefiles []string
	ReduceId    int
	StartTime   int64
	State       int
}

// define Master state
type MasterState int

const (
	CoIdle     MasterState = 1
	CoDomap    MasterState = 2
	CoDoreduce MasterState = 3
	CoDone     MasterState = 4
)

type Master struct {
	// Your definitions here.
	CoState      MasterState
	CoMapList    []*MapTask
	CoReduceList []*ReduceTask
	L            *sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Master) GetTask(args *RequestArgs, reply *TaskReply) error {
	c.L.Lock()
	defer c.L.Unlock()
	CurCoState := c.CoState
	if CurCoState == CoDomap {
		for _, MapTask := range c.CoMapList {
			if MapTask.State == 0 {
				reply.MapTask = MapTask
				reply.TaskType = 1
				MapTask.State = 1
				return nil
			}
		}
	} else {
		for _, ReduceTask := range c.CoReduceList {
			if ReduceTask.State == 0 {
				reply.ReduceTask = ReduceTask
				reply.TaskType = 2
				ReduceTask.State = 1
				return nil
			}
		}
	}
	return nil
}

func (c *Master) TaskDone(args *TaskReply, res *string) error {
	c.L.Lock()
	isDone := true
	if args.TaskType == 1 {
		for _, MapTask := range c.CoMapList {
			if MapTask.State != 2 {
				if MapTask.MapId == args.MapTask.MapId {
					MapTask.State = 2
				} else {
					isDone = false
				}
			}
		}
		if isDone {
			c.CoState = CoDoreduce
		}
	} else if args.TaskType == 2 {
		for _, ReduceTask := range c.CoReduceList {
			if ReduceTask.State != 2 {
				if ReduceTask.ReduceId == args.ReduceTask.ReduceId {
					ReduceTask.State = 2
				} else {
					isDone = false
				}
			}
		}
		if isDone {
			c.CoState = CoDone
		}
	}
	c.L.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Master) server() {
	rpc.Register(c)
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
func (c *Master) Done() bool {
	ret := false
	c.L.Lock()
	CurCoState := c.CoState
	c.L.Unlock()
	// Your code here.
	if CurCoState == CoDone {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	c := Master{}
	// Your code here.
	c.CoState = CoDomap
	c.L = new(sync.Mutex)
	// Initialize maplist
	c.CoMapList = make([]*MapTask, len(files))
	for i := 0; i < len(files); i++ {
		c.CoMapList[i] = new(MapTask)
		c.CoMapList[i].MapId = i
		c.CoMapList[i].Mapfiles = append(c.CoMapList[i].Mapfiles, files[i])
		c.CoMapList[i].State = 0
		c.CoMapList[i].StartTime = time.Now().Unix()
	}

	// Initialize reducelist
	c.CoReduceList = make([]*ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.CoReduceList[i] = new(ReduceTask)
		c.CoReduceList[i].ReduceId = i
		c.CoReduceList[i].State = 0
		c.CoReduceList[i].StartTime = time.Now().Unix()
	}

	c.server()
	return &c
}
