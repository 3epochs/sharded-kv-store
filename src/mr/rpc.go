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

type TaskType int
const (
	MapTask TaskType = 1
	ReduceTask TaskType = 2
)

// Add your RPC definitions here.
type Request struct {

}

type Response struct {
	NMap int
	NReduce int  // reduce task number, used for generating intermediate files
	Type TaskType
	FileName string
	Number int
}

type TaskFinishReq struct {
	Type TaskType
	Number int
}

type TaskFinishResp struct {
	Done bool
}

//type JobDoneReq struct {
//
//}
//
//type JobDoneResp struct {
//	Done bool
//}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
