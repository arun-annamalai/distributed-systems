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
	mapJobs     []Job
	mapJobsM    sync.Mutex
	reduceJobs  []Job
	reduceJobsM sync.Mutex

	stage  Stage
	stageM sync.Mutex

	completedReduceJobs  map[int]bool
	completedReduceJobsM sync.Mutex

	completedMapJobs  map[int]bool
	completedMapJobsM sync.Mutex

	totalReduceJobs int
	totalMapJobs    int

	taskReassignDuration time.Duration
}

type Job struct {
	FileNames   []string
	TimeStarted time.Time
	IsMapJob    bool
	JobNumber   int
	NReduce     int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	log.Println("worker: " + args.Uuid + " registered by master")
	reply.Success = true
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	log.Println("master waiting on stage lock")
	c.stageM.Lock()
	defer c.stageM.Unlock()

	log.Print("worker: " + args.Uuid + " requesting task" + "Stage: ")
	log.Printf("%#v\n", c.stage)
	reply.WorkerWait = false
	reply.JobDone = false

	if c.stage == Map {
		c.assignJob(&c.mapJobs, &c.mapJobsM, args, reply)
	} else if c.stage == Reduce {
		log.Println("Here")
		c.assignJob(&c.reduceJobs, &c.reduceJobsM, args, reply)
	} else {
		reply.WorkerWait = true
	}
	reply.Success = true
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {

	var completedJobListM *sync.Mutex
	var jobList map[int]bool

	changeStage := false
	reply.Success = true
	if args.IsMapJob {
		completedJobListM = &c.completedMapJobsM
		jobList = c.completedMapJobs
	} else {
		completedJobListM = &c.completedReduceJobsM
		jobList = c.completedReduceJobs
	}

	completedJobListM.Lock()
	jobList[args.TaskNumber] = true

	if args.IsMapJob {
		if len(jobList) == c.totalMapJobs {
			log.Println("Map stage completed | Creating reduce tasks")
			completedJobListM.Unlock()
			err := c.createReduceTasks()
			if err != nil {
				log.Fatal(err)
			}
			changeStage = true
		} else {
			completedJobListM.Unlock()
		}
	} else {
		if len(c.completedReduceJobs) == c.totalReduceJobs {
			changeStage = true
		}
		completedJobListM.Unlock()
	}

	if changeStage {
		c.changeStage()
	}

	return nil
}

func (c *Coordinator) changeStage() {
	c.stageM.Lock()
	defer c.stageM.Unlock()

	if c.stage == Map {
		log.Println("Changing stage to reduce")
		c.stage = Reduce
	} else {
		log.Println("Changing stage to done")
		c.stage = Done
	}

}

func (c *Coordinator) assignJob(jobList *[]Job, jobListLock *sync.Mutex, args *RequestTaskArgs, reply *RequestTaskReply) {
	jobListLock.Lock()
	if len(*jobList) == 0 {
		reply.WorkerWait = true
		reply.Success = true
		jobListLock.Unlock()
		return
	}

	// assign a job to worker
	job := (*jobList)[len(*jobList)-1]
	job.TimeStarted = time.Now()
	*jobList = (*jobList)[0 : len(*jobList)-1]
	reply.Job = job
	if len(job.FileNames) == 1 {
		log.Print("\tGiving worker map task: " + job.FileNames[0] + "\n")
	} else {
		log.Printf("\tGiving worker reduce tasks: %#v\n", job.FileNames)
	}
	jobListLock.Unlock()
	//time.Sleep(c.taskReassignDuration)
	//if !c.completedMapJobs[job.JobNumber] {
	//	jobListLock.Lock()
	//	defer jobListLock.Unlock()
	//	jobList = append(jobList, job)
	//}
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
	log.Println("Finished creating reduce tasks")
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
	c.completedMapJobs = make(map[int]bool)
	c.completedReduceJobs = make(map[int]bool)
	sort.Strings(files)
	for idx, file := range files {
		job := Job{}
		job.FileNames = append(job.FileNames, file)
		job.JobNumber = idx
		job.IsMapJob = true
		job.NReduce = nReduce
		c.mapJobs = append(c.mapJobs, job)
	}
	c.totalMapJobs = len(files)
	c.taskReassignDuration = time.Duration(10) * time.Second

	c.server()
	log.Print("Coordinator running...\n")
	log.Printf("\t %d files found", c.totalMapJobs)
	return &c
}
