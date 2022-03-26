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

const (
	logging         bool = false
	faultTolerance       = false
	faultTolerance2      = false
)

type Coordinator struct {
	// Your definitions here.
	mapJobs     []Job
	mapJobsM    sync.Mutex
	reduceJobs  []Job
	reduceJobsM sync.Mutex

	stage  Stage
	stageM sync.Mutex

	completedReduceJobs  *set
	completedReduceJobsM sync.Mutex

	completedMapJobs  *set
	completedMapJobsM sync.Mutex

	totalReduceJobs int
	totalMapJobs    int

	taskReassignDuration int64

	//registeredWorkers  map[string]Job
	//registeredWorkersM sync.Mutex
}

type Job struct {
	FileNames            []string
	TimeStarted          time.Time
	IsMapJob             bool
	JobNumber            int
	NReduce              int
	taskReassignDuration int64
}

func (j *Job) isExpired() bool {
	return time.Since(j.TimeStarted).Milliseconds() > j.taskReassignDuration
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

	log.Printf("worker: "+args.Uuid+" requesting task "+"Stage: %#v", c.stage)
	reply.WorkerWait = false
	reply.JobDone = false

	if c.stage == Map {
		c.stageM.Unlock()
		c.assignJob(&c.mapJobs, &c.mapJobsM, c.completedMapJobs, &c.completedMapJobsM, args, reply)
	} else if c.stage == Reduce {
		c.stageM.Unlock()
		c.assignJob(&c.reduceJobs, &c.reduceJobsM, c.completedReduceJobs, &c.completedReduceJobsM, args, reply)
	} else {
		c.stageM.Unlock()
		reply.WorkerWait = true
	}
	reply.Success = true
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	var completedJobListM *sync.Mutex
	var completedJobSet *set

	changeStage := false
	reply.Success = true
	if args.IsMapJob {
		completedJobListM = &c.completedMapJobsM
		completedJobSet = c.completedMapJobs
	} else {
		completedJobListM = &c.completedReduceJobsM
		completedJobSet = c.completedReduceJobs
	}

	completedJobListM.Lock()

	// handle repeat jobs successful
	if completedJobSet.Contains(args.TaskNumber) {
		completedJobListM.Unlock()
		return nil
	}

	completedJobSet.Add(args.TaskNumber)

	if args.IsMapJob {
		if completedJobSet.Size() == c.totalMapJobs {
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
		if completedJobSet.Size() == c.totalReduceJobs {
			log.Printf("\tThis is the state of completed reducejobs: %#v\n", completedJobSet)
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

func (c *Coordinator) assignJob(jobList *[]Job, jobListLock *sync.Mutex, completedJobs *set, completedJobsM *sync.Mutex, args *RequestTaskArgs, reply *RequestTaskReply) {
	//c.registeredWorkersM.Lock()
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

	//c.registeredWorkers[args.Uuid] = job
	//c.registeredWorkersM.Unlock()
	if len(job.FileNames) == 1 {
		log.Print("\tGiving worker map task: " + job.FileNames[0] + "\n")
	} else {
		log.Printf("\tGiving worker reduce tasks: %#v\n", job.FileNames)
	}
	jobListLock.Unlock()

	//if faultTolerance {
	//	time.Sleep(c.taskReassignDuration)
	//	completedJobsM.Lock()
	//	defer completedJobsM.Unlock()
	//
	//	if !completedJobs.Contains(job.JobNumber) {
	//		jobListLock.Lock()
	//		defer jobListLock.Unlock()
	//		*jobList = append(*jobList, job)
	//	}
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
		job.taskReassignDuration = c.taskReassignDuration
		c.reduceJobs = append(c.reduceJobs, job)
	}
	log.Println("Finished creating reduce tasks")
	return nil
}

//func (c *Coordinator) faultTolerance() {
//
//	for {
//		// TODO: Look here
//		c.stageM.Lock()
//		stage := c.stage
//		c.stageM.Unlock()
//
//		if stage == Done {
//			break
//		}
//
//		c.registeredWorkersM.Lock()
//
//		for _, job := range c.registeredWorkers {
//
//			var completedJobListM *sync.Mutex
//			var completedJobSet *set
//			var jobListM *sync.Mutex
//			var jobList *[]Job
//
//			if job.IsMapJob {
//				completedJobListM = &c.completedMapJobsM
//				completedJobSet = c.completedMapJobs
//				jobListM = &c.mapJobsM
//				jobList = &c.mapJobs
//
//			} else {
//				completedJobListM = &c.completedReduceJobsM
//				completedJobSet = c.completedReduceJobs
//				jobListM = &c.reduceJobsM
//				jobList = &c.reduceJobs
//			}
//
//			completedJobListM.Lock()
//
//			if job.isExpired() && !completedJobSet.Contains(job.JobNumber) {
//				jobListM.Lock()
//				*jobList = append(*jobList, job)
//				jobListM.Unlock()
//			}
//
//			completedJobListM.Unlock()
//		}
//		c.registeredWorkersM.Unlock()
//		time.Sleep(1)
//	}
//}

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
	if !logging {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	c := Coordinator{}

	// Your code here.
	c.totalReduceJobs = nReduce
	c.stage = Map
	c.completedMapJobs = NewSet()
	c.completedReduceJobs = NewSet()
	sort.Strings(files)
	for idx, file := range files {
		job := Job{}
		job.FileNames = append(job.FileNames, file)
		job.JobNumber = idx
		job.IsMapJob = true
		job.NReduce = nReduce
		job.taskReassignDuration = c.taskReassignDuration
		c.mapJobs = append(c.mapJobs, job)
	}
	c.totalMapJobs = len(files)
	c.taskReassignDuration = 10 * 1000
	//c.registeredWorkers = make(map[string]Job)

	c.server()
	log.Print("Coordinator running...\n")
	log.Printf("\t %d files found", c.totalMapJobs)
	return &c
}

type set struct {
	m map[int]bool
}

func NewSet() *set {
	s := &set{}
	s.m = make(map[int]bool)
	return s
}

func (s *set) Add(value int) {
	s.m[value] = true
}

func (s *set) Remove(value int) {
	delete(s.m, value)
}

func (s *set) Contains(value int) bool {
	_, exists := s.m[value]
	if !exists {
		delete(s.m, value)
	}
	return exists
}

func (s *set) Size() int {
	return len(s.m)
}
