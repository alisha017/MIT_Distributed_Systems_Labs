package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// TaskType is an enumeration of task types that can be assigned to workers.
type TaskType int

const (
	MAP TaskType = iota // iota starts at 0 and increments for each item in the list
	REDUCE
	DONE
	WAITING
)

func (t TaskType) String() string {
	switch t {
	case MAP:
		return "Map"
	case REDUCE:
		return "Reduce"
	case DONE:
		return "Done"
	case WAITING:
		return "Waiting"
	default:
		return "Unknown Task"
	}
}

type AskForTaskArgs struct {
	WorkerId string
}

type AskForTaskReply struct {
	FileLoc []string
	Task    TaskType
	NReduce int
	RTask   int
	NMap    int
}

type GetWorkerIdArgs struct {
	// empty because no data is needed.
}

type GetWorkerIdReply struct {
	WorkerId string
}

type TaskCompleteArgs struct {
	WorkerID      string
	Task          TaskType
	FileProcessed []string
	OutputFiles   []string
	Success       bool
	RTask         int
}

type TaskCompleteReply struct {
	Acknowledged bool
}

// WorkerShutdownArgs Define the arguments and reply types for the WorkerShutdown RPC
type WorkerShutdownArgs struct {
	WorkerID string
}

type WorkerShutdownReply struct {
	Acknowledged bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
