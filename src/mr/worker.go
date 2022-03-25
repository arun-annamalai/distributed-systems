package mr

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	// Your worker implementation here.

	Uuid := uuid.New().String()

	registerWorkerReply, ok := RegisterWorker(Uuid)
	if !ok {
		panic("unable to register worker")
	}
	if !registerWorkerReply.Success {
		panic("register worker failed")
	}

	for {
		requestTaskReply, ok := RequestTask(Uuid)
		if !ok {
			return
		}
		if requestTaskReply.WorkerWait {
			log.Println("Worker: " + Uuid + " waiting")
			time.Sleep(3000 * time.Millisecond)
			continue
		}
		doTask(mapf, reducef, requestTaskReply)
	}

}

//
// function to call mr/coordinator::RegisterWorker rpc function
//
// the RPC argument and reply types are defined in rpc.go.
//
func RegisterWorker(Uuid string) (RegisterWorkerReply, bool) {

	// declare an argument structure.
	args := RegisterWorkerArgs{}

	// fill in the argument(s).
	args.Uuid = Uuid

	// declare a reply structure.
	reply := RegisterWorkerReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		log.Printf("reply.Success %v\n", reply.Success)
	} else {
		log.Printf("call failed!\n")
		println("unable to register worker")
	}
	return reply, ok
}

func RequestTask(Uuid string) (RequestTaskReply, bool) {
	log.Print("worker: " + Uuid + " requesting task\n")
	// declare an argument structure.
	args := RequestTaskArgs{}
	args.Uuid = Uuid

	// declare a reply structure.
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		log.Printf("reply.Success %v\n", reply.Success)
	} else {
		log.Printf("call failed!\n")
		return RequestTaskReply{}, false
	}
	log.Printf("%#v\n", reply)
	return reply, ok
}

func doTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, requestTaskReply RequestTaskReply) {
	log.Printf("Worker doing task\n")

	// declare an argument structure.
	args := TaskDoneArgs{}
	args.TaskNumber = requestTaskReply.Job.JobNumber
	args.IsMapJob = requestTaskReply.Job.IsMapJob

	// declare a reply structure.
	reply := TaskDoneReply{}

	if requestTaskReply.Job.IsMapJob {
		filename := requestTaskReply.Job.FileNames[0]
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open map input file %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read map input file%v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		mapTaskNumber := requestTaskReply.Job.JobNumber

		for _, kv := range kva {
			reduceTaskNumber := ihash(kv.Key) % requestTaskReply.Job.NReduce
			reduceFileName := fmt.Sprintf("mr-%d-%d", mapTaskNumber, reduceTaskNumber)
			file, err := os.OpenFile(reduceFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}
			enc := json.NewEncoder(file)
			err = enc.Encode(&kv)
			if err != nil {
				log.Printf("cannot write to map encoder file %v", file)
				log.Fatal(err)
			}
			err = file.Close()
			if err != nil {
				log.Fatal(err)
			}
		}

		ok := call("Coordinator.TaskDone", &args, &reply)
		if ok {
			log.Printf("reply.Success %v\n", reply.Success)
		} else {
			log.Printf("call failed!\n")
		}
	} else {
		var intermediate []KeyValue
		var kv KeyValue
		reduceTaskNumber := requestTaskReply.Job.JobNumber

		for _, filename := range requestTaskReply.Job.FileNames {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatal(err)
			}
			dec := json.NewDecoder(file)
			for {
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			err = file.Close()
			if err != nil {
				log.Fatal(err)
			}
		}

		sort.Sort(ByKey(intermediate))

		wd, err := os.Getwd()

		if err != nil {
			log.Fatal(err)
		}

		ofile, err := os.CreateTemp(wd, fmt.Sprintf("temp-reducer-file-%d", reduceTaskNumber))
		if err != nil {
			log.Fatal(err)
		}

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			var values []string
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		err = ofile.Close()
		if err != nil {
			log.Fatal(err)
		}

		err = os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", reduceTaskNumber))
		if err != nil {
			log.Fatal(err)
		}

		ok := call("Coordinator.TaskDone", &args, &reply)
		if ok {
			log.Printf("reply.Success %v\n", reply.Success)
		} else {
			log.Printf("call failed!\n")
		}
	}

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Print("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
