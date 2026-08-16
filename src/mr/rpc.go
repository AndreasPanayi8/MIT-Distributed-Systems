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
	// WaitTask is first so it is the zero value: a failed or empty
	// GetTaskReply decodes to "wait and retry", never to real work.
	WaitTask TaskType = iota
	MapTask
	ReduceTask
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
