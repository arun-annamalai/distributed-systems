package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sort"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Stage int64

const (
	Map Stage = iota
	Reduce
	Done
)

type Coordinator struct {
	// Your definitions here.
	mapJobs            []Job
	mapJobsM           sync.Mutex
	reduceJobs         []Job
	reduceJobsM        sync.Mutex
	registeredWorkers  map[string]Job
	registeredWorkersM sync.Mutex

	stage  Stage
	stageM sync.Mutex

	completedReduceJobs  int
	completedReduceJobsM sync.Mutex
	completedMapJobs     int
	completedMapJobsM    sync.Mutex

	totalReduceJobs int
	totalMapJobs    int
}

type Job struct {
	FileNames   []string
	TimeStarted time.Time
	IsMapJob    bool
	JobNumber   int
	NReduce     int
}

type concurrentInt struct {
	value int
	mutex sync.Mutex
}

func (c *concurrentInt) setVal(newVal int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.value = newVal

}

func (c *concurrentInt) getVal(newVal int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.value = newVal
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	log.Println(args.Uuid)
	reply.Success = true
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	log.Print("worker: " + args.Uuid + " requesting task\n")
	reply.WorkerWait = false
	reply.JobDone = false

	c.stageM.Lock()
	defer c.stageM.Unlock()

	if c.stage == Map {
		c.mapJobsM.Lock()
		defer c.mapJobsM.Unlock()

		if len(c.mapJobs) == 0 {
			reply.WorkerWait = true
			reply.Success = true
			return nil
		}

		// assign a job to worker
		job := c.mapJobs[len(c.mapJobs)-1]
		job.TimeStarted = time.Now()
		c.mapJobs = c.mapJobs[0 : len(c.mapJobs)-1]
		c.registeredWorkersM.Lock()
		c.registeredWorkers[args.Uuid] = job
		c.registeredWorkersM.Unlock()
		reply.Job = job
		log.Print("\tGiving worker map task: " + job.FileNames[0] + "\n")
	} else if c.stage == Reduce {
		c.reduceJobsM.Lock()
		defer c.reduceJobsM.Unlock()

		if len(c.reduceJobs) == 0 {
			reply.WorkerWait = true
			reply.Success = true
			return nil
		}

		// assign a job to worker
		job := c.reduceJobs[len(c.reduceJobs)-1]
		job.TimeStarted = time.Now()
		c.reduceJobs = c.reduceJobs[0 : len(c.reduceJobs)-1]
		c.registeredWorkers[args.Uuid] = job
		reply.Job = job
		log.Printf("\tGiving worker reduce tasks: %#v\n", job.FileNames)
	} else {
		reply.WorkerWait = true
	}
	reply.Success = true
	return nil
}

func (c *Coordinator) MapTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	reply.Success = true

	c.completedMapJobsM.Lock()
	defer c.completedMapJobsM.Unlock()

	c.completedMapJobs += 1
	// if last map task, then change stage to reduce
	if c.completedMapJobs == c.totalMapJobs {
		log.Print("Map stage completed | Creating reduce tasks")
		err := c.createReduceTasks()
		if err != nil {
			log.Fatal(err)
		}

		c.stageM.Lock()
		c.stage = Reduce
		c.stageM.Unlock()
	}
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	reply.Success = true

	c.completedReduceJobsM.Lock()
	defer c.completedReduceJobsM.Unlock()

	c.completedReduceJobs += 1
	// if last map task, then change stage to reduce
	if c.completedReduceJobs == c.totalReduceJobs {
		log.Print("Reduce stage completed")
		c.stageM.Lock()
		c.stage = Done
		c.stageM.Unlock()

	}
	return nil
}

func (c *Coordinator) createReduceTasks() error {
	c.reduceJobsM.Lock()
	defer c.reduceJobsM.Unlock()

	for i := 0; i < c.totalReduceJobs; i++ {
		files, err := filepath.Glob(fmt.Sprintf("mr-*-%d", i))
		if err != nil {
			log.Fatal(err)
		}

		job := Job{}
		job.FileNames = append(job.FileNames, files...)
		job.JobNumber = i
		job.IsMapJob = false
		job.NReduce = c.totalReduceJobs
		c.reduceJobs = append(c.reduceJobs, job)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.stageM.Lock()
	ret := c.stage == Done
	defer c.stageM.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)
	c := Coordinator{}

	// Your code here.
	c.totalReduceJobs = nReduce
	c.stage = Map
	c.completedMapJobs = 0
	c.completedReduceJobs = 0
	sort.Strings(files)
	for idx, file := range files {
		job := Job{}
		job.FileNames = append(job.FileNames, file)
		job.JobNumber = idx
		job.IsMapJob = true
		job.NReduce = nReduce
		c.mapJobs = append(c.mapJobs, job)
	}
	c.registeredWorkers = make(map[string]Job)
	c.totalMapJobs = len(files)

	c.server()
	log.Print("Coordinator running...\n")
	log.Printf("\t %d files found", c.totalMapJobs)
	return &c
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
