package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	nMap int
	nReduce int
	mutex sync.Mutex
	mapTasksToAssign map[int]string  // map task number to filename
	mapTasksAssigned map[int]MapTaskStatus  // map task number to status

	reduceTasksToAssign map[int]bool
	reduceTasksAssigned map[int]ReduceTaskStatus
}

type MapTaskStatus struct {
	filename string
	timeStamp int64
}

type ReduceTaskStatus struct {
	timeStamp int64
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(req *Request, resp *Response) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	resp.NMap = m.nMap
	resp.NReduce = m.nReduce
	// map task
	if len(m.mapTasksToAssign) > 0 || len(m.mapTasksAssigned) > 0 {
		// check 10 sec limits
		filename := ""
		number := -1
		if len(m.mapTasksToAssign) > 0 {
			for key, val := range m.mapTasksToAssign {
				number = key
				filename = val
				break
			}
			if filename != "" && number != -1 {
				delete(m.mapTasksToAssign, number)
				status := MapTaskStatus{
					filename: filename,
					timeStamp: time.Now().Unix(),
				}
				m.mapTasksAssigned[number] = status
			}
		} else {
			for key, status := range m.mapTasksAssigned {
				if time.Now().Unix() - status.timeStamp >= 10 {
					number = key
					filename = status.filename
					status.timeStamp = time.Now().Unix()
					break
				}
			}
		}
		resp.FileName = filename
		resp.Number = number
		resp.Type = MapTask
		return nil
	}
	// reduce task
	if len(m.reduceTasksToAssign) > 0 || len(m.reduceTasksAssigned) > 0 {
		number := -1
		if len(m.reduceTasksToAssign) > 0 {
			for idx, _ := range m.reduceTasksToAssign {
				number = idx
				break
			}
			if number != -1 {
				delete(m.reduceTasksToAssign, number)
				status := ReduceTaskStatus{
					timeStamp: time.Now().Unix(),
				}
				m.reduceTasksAssigned[number] = status
			}
		} else {
			for key, status := range m.reduceTasksAssigned {
				if time.Now().Unix() - status.timeStamp >= 10 {
					number = key
					status.timeStamp = time.Now().Unix()
					break
				}
			}
		}
		resp.FileName = ""
		resp.Number = number
		resp.Type = ReduceTask
		return nil
	}
	return nil
}

func (m *Master) WorkerFinishTask(req *TaskFinishReq, resp *TaskFinishResp) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if req.Type == MapTask {
		if _, ok := m.mapTasksAssigned[req.Number]; ok {
			delete(m.mapTasksAssigned, req.Number)
		}
		resp.Done = false
	} else {
		if _, ok := m.reduceTasksAssigned[req.Number]; ok {
			delete(m.reduceTasksAssigned, req.Number)
		}
		if len(m.reduceTasksToAssign) == 0 && len(m.reduceTasksAssigned) == 0 {
			resp.Done = true
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
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
func (m *Master) Done() bool {
	// Your code here.
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return len(m.mapTasksToAssign) == 0 && len(m.mapTasksAssigned) == 0 &&
		len(m.reduceTasksToAssign) == 0 && len(m.reduceTasksAssigned) == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	mapTasksToAssign := make(map[int]string)
	for i, filename := range files {
		mapTasksToAssign[i + 1] = filename
	}
	reduceTasksToAssign := make(map[int]bool)
	for i := 0; i < nReduce; i++ {
		reduceTasksToAssign[i + 1] = true
	}
	m := Master{
		nMap: len(files),
		nReduce: nReduce,
		mapTasksToAssign: mapTasksToAssign,
		reduceTasksToAssign: reduceTasksToAssign,
	}
	m.server()
	return &m
}
