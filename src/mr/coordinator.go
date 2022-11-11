package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Task struct {
	file     []string
	status   int //0:unallocated,1:processing,2:completed
	worker   string
	taskType int //0:map,1:reduce
}

//hint:modify the coordinator to respond with the file name of an as-yet-unstarted map task.
type Coordinator struct {
	// Your definitions here.
	WorkerID       int
	nReduce        int
	mapCount       int
	reduceCount    int
	MapTasks       []*Task
	mapFinished    bool
	ReduceTasks    []*Task
	reduceFinished bool
	mu             sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Finish(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceFinished {
		return errors.New("all finished")
	}
	if args.Task == "MAP" {
		for i := 0; i < len(c.MapTasks); i++ {
			mt := c.MapTasks[i]
			if mt.status == 1 && mt.worker == args.ID {
				mt.status = 2
				c.mapCount++
				break
			}
		}
		if c.mapCount == len(c.MapTasks) {
			c.arrangeReduce()
			c.mapFinished = true
		}
	} else if args.Task == "REDUCE" {
		for i := 0; i < len(c.ReduceTasks); i++ {
			rt := c.ReduceTasks[i]
			if rt.status == 1 && rt.worker == args.ID {
				rt.status = 2
				c.reduceCount++
				break
			}
		}
		if c.reduceCount == len(c.ReduceTasks) {
			c.reduceFinished = true
			return errors.New("all finished")
		}
	} else {
		reply.Message = "no such task type!"
	}
	return nil
}

func (c *Coordinator) arrangeReduce() {
	//for all map tasks, get the intermediate flies mr-X-Y
	workers := []string{}
	for _, mt := range c.MapTasks {
		workers = append(workers, mt.worker)
	}
	for i := 0; i < c.nReduce; i++ {
		buf := []string{}
		for _, w := range workers {
			filename := "mr-" + w + "-" + strconv.Itoa(i) + ".txt"
			buf = append(buf, filename)
		}
		c.ReduceTasks[i].file = buf
	}

}

func (c *Coordinator) monitor(task *Task) {
	//check after 10s
	time.Sleep(10 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	if task.status != 2 {
		task.status = 0
		task.worker = ""
	}
}

func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceFinished {
		return errors.New("all finished")
	}
	if !c.mapFinished {
		mt := &Task{}
		found := false
		for i := 0; i < len(c.MapTasks); i++ {
			if c.MapTasks[i].status == 0 {
				mt = c.MapTasks[i]
				found = true
				break
			}
		}
		if !found {
			reply.Task = "WAIT"
			return nil
		}
		// fmt.Println(c.MapTasks)
		reply.Filename = mt.file[0]
		reply.ID = strconv.Itoa(c.WorkerID)
		c.WorkerID++
		reply.R = c.nReduce
		reply.Task = "MAP"
		reply.Message = "do the map task"
		mt.status = 1
		mt.worker = reply.ID
		go c.monitor(mt)
		return nil
	}
	if !c.reduceFinished {
		//allocated reduce task
		rt := &Task{}
		for i := 0; i < len(c.ReduceTasks); i++ {
			if c.ReduceTasks[i].status == 0 {
				rt = c.ReduceTasks[i]
				reply.ID = strconv.Itoa(i)
				break
			}
		}
		rt.worker = reply.ID
		rt.status = 1
		reply.Filenames = rt.file
		reply.Task = "REDUCE"
		reply.Message = "do the reduce task"
		go c.monitor(rt)
	}

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
	c.mu.Lock()
	defer c.mu.Unlock()

	// Your code here.

	return c.reduceFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.WorkerID = 0
	c.nReduce = nReduce
	c.mapFinished = false
	c.reduceFinished = false
	c.MapTasks = make([]*Task, len(files))
	for i := 0; i < len(files); i++ {
		c.MapTasks[i] = &Task{}
		buf := []string{files[i]}
		c.MapTasks[i].file = buf
		// fmt.Println(c.MapTasks[i].file[0])
		c.MapTasks[i].status = 0
		c.MapTasks[i].taskType = 0
	}
	c.ReduceTasks = make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[i] = &Task{}
		c.ReduceTasks[i].status = 0
		c.ReduceTasks[i].taskType = 1
	}
	// Your code here.

	c.server()
	return &c
}
