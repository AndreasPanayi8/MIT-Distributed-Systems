package mr

import (
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		task := CallGetTask()
		if task.Type == ExitTask {
			break
		}
		if task.Type == MapTask {
			CallDoMapTask(task, mapf)
		} else if task.Type == ReduceTask {
			CallDoReduceTask(task, reducef)
		} else if task.Type == WaitTask {
			// wait for a second before asking for a new task
			time.Sleep(time.Second)
		}
	}
}

// the RPC argument and reply types are defined in rpc.go.
func CallGetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Printf("%d: CallGetTask failed", os.Getpid())
	}
	return reply
}

func CallReport(task GetTaskReply) {
	args := ReportArgs{Type: task.Type, TaskID: task.TaskID}
	reply := ReportReply{}
	ok := call("Coordinator.Report", &args, &reply)
	if !ok {
		log.Printf("%d: CallReport failed", os.Getpid())
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func CallDoMapTask(task GetTaskReply, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(task.File)
	if err != nil {
		log.Fatalf("cannot read %v: %v", task.File, err)
	}

	// call the map function
	kva := mapf(task.File, string(content))

	// write intermediate files
	intermediateFiles := make([]*os.File, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		// "." so the temp lands on the same filesystem as its final name:
		// os.Rename cannot cross a mount point.
		intermediateFiles[i], err = os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file: %v", err)
		}
	}
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % task.NReduce
		_, err := intermediateFiles[reduceTaskNum].WriteString(kv.Key + "\t" + kv.Value + "\n")
		if err != nil {
			log.Fatalf("cannot write to temp file: %v", err)
		}
	}

	for i := 0; i < task.NReduce; i++ {
		// close the temp file
		err := intermediateFiles[i].Close()
		if err != nil {
			log.Fatalf("cannot close temp file: %v", err)
		}
	}

	for i := 0; i < task.NReduce; i++ {
		// rename the temp file to the final name
		finalName := "mr-" + strconv.Itoa(task.TaskID) + "-" + strconv.Itoa(i)
		err := os.Rename(intermediateFiles[i].Name(), finalName)
		if err != nil {
			log.Fatalf("cannot rename temp file: %v", err)
		}
	}

	// report completion of map task
	CallReport(task)

}

func CallDoReduceTask(task GetTaskReply, reducef func(string, []string) string) {
	// read intermediate files
	intermediate := make(map[string][]string)
	for i := 0; i < task.NMap; i++ {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskID)
		content, err := os.ReadFile(fileName)
		if err != nil {
			log.Fatalf("cannot read %v: %v", fileName, err)
		}
		lines := string(content)
		for _, line := range strings.Split(lines, "\n") {
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, "\t", 2)
			intermediate[parts[0]] = append(intermediate[parts[0]], parts[1])
		}
	}

	// sort keys
	var keys []string
	for k := range intermediate {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// write output file
	tmpFile, err := os.CreateTemp(".", "mr-tmp-out*")
	if err != nil {
		log.Fatalf("cannot create temp file: %v", err)
	}
	for _, k := range keys {
		output := reducef(k, intermediate[k])
		_, err := tmpFile.WriteString(k + " " + output + "\n")
		if err != nil {
			log.Fatalf("cannot write to temp file: %v", err)
		}
	}
	err = tmpFile.Close()
	if err != nil {
		log.Fatalf("cannot close temp file: %v", err)
	}

	finalName := "mr-out-" + strconv.Itoa(task.TaskID)
	err = os.Rename(tmpFile.Name(), finalName)
	if err != nil {
		log.Fatalf("cannot rename temp file: %v", err)
	}

	// report completion of reduce task
	CallReport(task)
}
