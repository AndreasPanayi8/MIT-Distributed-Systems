package mr

// RPC definitions.
//
// remember to capitalize all names.
//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type GetTaskArgs struct{}

type GetTaskReply struct {
	Type    TaskType
	TaskID  int
	File    string
	NReduce int
	NMap    int
}

type ReportArgs struct {
	Type   TaskType
	TaskID int
}

type ReportReply struct{}
