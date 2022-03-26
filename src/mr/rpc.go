package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// RPC struct definitions for registering a worker

type RegisterWorkerArgs struct {
	Uuid string
}

type RegisterWorkerReply struct {
	Success bool
}

// RPC struct definitions for requesting a tasks

type RequestTaskArgs struct {
	Uuid string
}

type RequestTaskReply struct {
	Success    bool
	WorkerWait bool
	JobDone    bool
	Job        Job
}

type TaskDoneArgs struct {
	TaskNumber int
	IsMapJob   bool
	Job        Job
}

type TaskDoneReply struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
