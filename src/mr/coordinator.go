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

type TaskStatus int

const (
	Pending TaskStatus = iota
	Assigned
	Completed
)

type TaskMetadata struct {
	Status    TaskStatus
	StartTime time.Time
}

type Coordinator struct {
	files       []string
	nReduce     int
	mapTasks    map[int]*TaskMetadata
	reduceTasks map[int]*TaskMetadata
	phase       TaskType
	mu          sync.Mutex
	done        bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.nReduce = nReduce
	c.mapTasks = make(map[int]*TaskMetadata)
	c.reduceTasks = make(map[int]*TaskMetadata)
	for i := range files {
		c.mapTasks[i] = &TaskMetadata{Status: Pending}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &TaskMetadata{Status: Pending}
	}
	c.phase = MapTask
	//	c.mu = sync.Mutex{}
	c.done = false

	c.server(sockname)
	return &c
}

func (c *Coordinator) allMapTasksCompleted() bool {
	for _, metadata := range c.mapTasks {
		if metadata.Status != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceTasksCompleted() bool {
	for _, metadata := range c.reduceTasks {
		if metadata.Status != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == MapTask {
		for taskID, metadata := range c.mapTasks {
			if metadata.Status == Pending {
				metadata.Status = Assigned
				metadata.StartTime = time.Now()
				reply.Type = MapTask
				reply.TaskID = taskID
				reply.File = c.files[taskID]
				reply.NReduce = c.nReduce
				reply.NMap = len(c.files)
				return nil
			} else if metadata.Status == Assigned && time.Since(metadata.StartTime) > 10*time.Second {
				metadata.StartTime = time.Now()
				reply.Type = MapTask
				reply.TaskID = taskID
				reply.File = c.files[taskID]
				reply.NReduce = c.nReduce
				reply.NMap = len(c.files)
				return nil
			}
		}
		if c.allMapTasksCompleted() {
			c.phase = ReduceTask
		} else {
			reply.Type = WaitTask
			return nil
		}
	}

	if c.phase == ReduceTask {
		for taskID, metadata := range c.reduceTasks {
			if metadata.Status == Pending {
				metadata.Status = Assigned
				metadata.StartTime = time.Now()
				reply.Type = ReduceTask
				reply.TaskID = taskID
				reply.NReduce = c.nReduce
				reply.NMap = len(c.files)
				return nil
			} else if metadata.Status == Assigned && time.Since(metadata.StartTime) > 10*time.Second {
				metadata.StartTime = time.Now()
				reply.Type = ReduceTask
				reply.TaskID = taskID
				reply.NReduce = c.nReduce
				reply.NMap = len(c.files)
				return nil
			}
		}
		if c.allReduceTasksCompleted() {
			c.phase = ExitTask
			c.done = true
			reply.Type = ExitTask
			return nil
		} else {
			reply.Type = WaitTask
			return nil
		}
	}

	return nil
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == MapTask {
		if metadata, ok := c.mapTasks[args.TaskID]; ok && metadata.Status == Assigned {
			metadata.Status = Completed
		}
	} else if args.Type == ReduceTask {
		if metadata, ok := c.reduceTasks[args.TaskID]; ok && metadata.Status == Assigned {
			metadata.Status = Completed
		}
	}

	return nil
}
