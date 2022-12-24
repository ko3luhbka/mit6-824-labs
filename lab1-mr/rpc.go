package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type (
	TaskReply struct {
		TaskID   int
		NReduce  int
		MapsNum int
		Filename string
		TaskType workType
		WorkerID int
	}

	WorkArgs struct {
		WorkerID int
	}

	TaskCompletedArgs struct {
		TaskKey  string
		TaskType workType
	}

	TaskCompletedReply struct {
		Confirmed bool
	}
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
