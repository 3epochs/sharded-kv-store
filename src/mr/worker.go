package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type fileEncoder struct {
	file *os.File
	encoder *json.Encoder
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// get task
	req := Request{}
	resp := Response{}
	success := call("Master.AssignTask", &req, &resp)
	for ; success;  {
		if resp.Type != 0 && resp.Number != 0 {
			finishReq := TaskFinishReq{}
			finishResp := TaskFinishResp{}
			// map
			if resp.Type == MapTask {
				// read file into mem
				filename := resp.FileName
				intermediate := []KeyValue{}
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("map phase cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("map phase cannot read %v", filename)
				}
				file.Close()

				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
				// write intermediate file to local disk
				// create files
				prefix := fmt.Sprintf("mr-%d", resp.Number)
				// log.Printf("map resp.Number=%d", resp.Number)
				prefix += "-%d"
				fileMap := make(map[string]*fileEncoder) // map filename to File
				for i := 1; i <= resp.NReduce; i++ {
					oname := fmt.Sprintf(prefix, i)
					ofile, _ := os.Create(oname)
					// log.Println("create file: ", oname)
					enc := json.NewEncoder(ofile)
					fileMap[oname] = &fileEncoder{
						file:    ofile,
						encoder: enc,
					}
				}
				// write file
				for _, kv := range intermediate {
					index := (ihash(kv.Key) % resp.NReduce) + 1
					filename := fmt.Sprintf(prefix, index)
					enc := fileMap[filename].encoder
					enc.Encode(&kv)
				}
				// close file
				for _, ofile := range fileMap {
					ofile.file.Close()
				}
				// task done, notify master
				finishReq.Type = MapTask
				finishReq.Number = resp.Number
			} else {
				// read intermediate files into mem
				reduceNum := resp.Number
				// log.Printf("reduce resp.Number=%d", resp.Number)
				kvs := []KeyValue{}
				formatStr := "mr-%d-%d"
				for i := 1; i <= resp.NMap; i++ {
					filename := fmt.Sprintf(formatStr, i, reduceNum)
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("reduce phase cannot open %v", filename)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kvs = append(kvs, kv)
					}
					file.Close()
				}
				sort.Sort(ByKey(kvs))
				// output file
				oname := fmt.Sprintf("mr-out-%d", reduceNum)
				ofile, _ := os.Create(oname)
				i := 0
				for i < len(kvs) {
					j := i + 1
					for j < len(kvs) && kvs[j].Key == kvs[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kvs[k].Value)
					}
					output := reducef(kvs[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

					i = j
				}
				finishReq.Type = ReduceTask
				finishReq.Number = reduceNum
			}
			call("Master.WorkerFinishTask", finishReq, finishResp)
			if finishResp.Done {
				return
			}
		} else {
			time.Sleep(time.Second)
		}
		success = call("Master.AssignTask", &req, &resp)
	}
	return
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
