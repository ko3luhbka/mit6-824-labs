package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	taskStatusTodo       = "todo"
	taskStatusInProgress = "inProgress"
	taskStatusDone       = "done"

	MapTask       workType = "map"
	ReduceTask    workType = "reduce"
	workerTimeout          = 10 * time.Second
)

type (
	Coordinator struct {
		nReduce     int
		mapTasks    map[string]*task
		mapMu       sync.RWMutex
		reduceTasks map[string]*task
		reduceMu    sync.RWMutex
	}

	task struct {
		id        int
		workerID  int
		startTime time.Time
		status    string
	}

	workType string
)

func (c *Coordinator) SetMap(key string, val *task) {
	c.mapMu.Lock()
	defer c.mapMu.Unlock()
	c.mapTasks[key] = val
}

func (c *Coordinator) SetReduce(key string, val *task) {
	c.reduceMu.Lock()
	defer c.reduceMu.Unlock()
	c.reduceTasks[key] = val
}

// check if any worker is executing a task longer than workerTimeout
// and in such a case make the task available for another worker to pick
func (c *Coordinator) checkWorkersAlive() {
	go func() {
		for {
			c.mapMu.RLock()
			for file, t := range c.mapTasks {
				now := time.Now()
				if t.status == taskStatusInProgress && now.After(t.startTime.Add(workerTimeout)) {
					fmt.Printf("worker %d is not responding, freeing task %s\n", t.workerID, file)
					resetT := &task{
						id:        t.id,
						workerID:  0,
						status:    taskStatusTodo,
						startTime: time.Time{},
					}
					c.mapMu.RUnlock()
					c.SetMap(file, resetT)
					c.mapMu.RLock()
				}
			}
			c.mapMu.RUnlock()
			time.Sleep(1 * time.Second)

			c.reduceMu.RLock()
			for file, t := range c.reduceTasks {
				now := time.Now()
				if t.status == taskStatusInProgress && now.After(t.startTime.Add(workerTimeout)) {
					fmt.Printf("worker %d is not responding, freeing task %s", t.workerID, file)
					resetT := &task{
						id:        t.id,
						workerID:  0,
						status:    taskStatusTodo,
						startTime: time.Time{},
					}
					c.reduceMu.RUnlock()
					c.SetReduce(file, resetT)
					c.reduceMu.RLock()
				}
			}
			c.reduceMu.RUnlock()
		}
	}()
}

func (c *Coordinator) GimmeWork(args *WorkArgs, reply *TaskReply) error {
	freeTask := c.getFreeTask()
	if freeTask == nil {
		return fmt.Errorf("no more tasks left!")
	}

	t := &task{
		id:        freeTask.TaskID,
		workerID:  args.WorkerID,
		startTime: time.Now(),
		status:    taskStatusInProgress,
	}

	switch freeTask.TaskType {
	case MapTask:
		c.SetMap(freeTask.Filename, t)
	case ReduceTask:
		c.SetReduce(freeTask.Filename, t)
	default:
		return fmt.Errorf("unknown task type: %s", freeTask.TaskType)
	}

	fmt.Printf("task %s is given to worker %d\n", freeTask.Filename, args.WorkerID)
	freeTask.WorkerID = args.WorkerID
	*reply = *freeTask
	return nil
}

func (c *Coordinator) MapTaskDone(args *TaskCompletedArgs, reply *TaskCompletedReply) error {
	c.mapMu.Lock()
	defer c.mapMu.Unlock()

	task, exists := c.mapTasks[args.TaskKey]
	if !exists {
		return fmt.Errorf("task %s doesn't exist in mapTasks", args.TaskKey)
	}

	task.status = taskStatusDone
	c.mapTasks[args.TaskKey] = task
	reply.Confirmed = true
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *TaskCompletedArgs, reply *TaskCompletedReply) error {
	c.reduceMu.Lock()
	defer c.reduceMu.Unlock()

	task, exists := c.reduceTasks[args.TaskKey]
	if !exists {
		return fmt.Errorf("task %s doesn't exist in reduceTasks", args.TaskKey)
	}

	task.status = taskStatusDone
	c.reduceTasks[args.TaskKey] = task
	reply.Confirmed = true
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mapMu.RLock()
	defer c.mapMu.RUnlock()
	for _, task := range c.mapTasks {
		if task.status != taskStatusDone {
			return false
		}
	}

	c.reduceMu.RLock()
	defer c.reduceMu.RUnlock()
	for _, task := range c.reduceTasks {
		if task.status != taskStatusDone {
			return false
		}
	}

	return true
}

func (c *Coordinator) getFreeTask() *TaskReply {
	// all map tasks should be processed before giving any reduce task to worker.
	// so first check if there are any free map tasks left
	c.mapMu.RLock()
	defer c.mapMu.RUnlock()

	for file, task := range c.mapTasks {
		if task.status == taskStatusTodo {
			return &TaskReply{
				TaskID:   task.id,
				NReduce:  c.nReduce,
				Filename: file,
				TaskType: MapTask,
			}
		}
	}

	// then check if there are any reduce tasks left
	c.reduceMu.RLock()
	defer c.reduceMu.RUnlock()

	for file, task := range c.reduceTasks {
		if task.status == taskStatusTodo {
			return &TaskReply{
				TaskID:   task.id,
				NReduce:  c.nReduce,
				MapsNum: len(c.mapTasks),
				Filename: file,
				TaskType: ReduceTask,
			}
		}
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:     nReduce,
		mapTasks:    make(map[string]*task),
		reduceTasks: make(map[string]*task),
	}

	for i, file := range files {
		c.mapTasks[file] = &task{
			id:     i,
			status: taskStatusTodo,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[strconv.Itoa(i)] = &task{
			id:     i,
			status: taskStatusTodo,
		}
	}

	c.checkWorkersAlive()

	c.server()
	return &c
}
