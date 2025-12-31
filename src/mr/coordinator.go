package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	MaxTaskRunInterval = time.Second * 10
)

type Task struct {
	filePath  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type Coordinator struct {
	files   []string
	nReduce int
	nMap    int
	phase   SchedulePhase
	tasks   []Task

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.
type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(reportRequest *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{reportRequest, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	c.tasks = make([]Task, c.nMap)
	for i, file := range c.files {
		c.tasks[i] = Task{
			filePath: file,
			id:       i,
			status:   Idle,
		}
	}
}

func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	for i := range c.tasks {
		c.tasks[i] = Task{
			id:     i,
			status: Idle,
		}
	}
}

func (c *Coordinator) initCompletePhase() {
	c.phase = CompletePhase
	c.doneCh <- struct{}{}
}

func (c *Coordinator) schedule() {
	c.initMapPhase()

	for {
		select {
		case msg := <-c.heartbeatCh:
			isAllTaskDoneInPhase := c.selectANewTask(msg.response)
			if isAllTaskDoneInPhase {
				c.switchPhase()
				c.selectTaskAfterSwitchPhase(msg.response)
			}
			log.Printf("Coordinator: Heartbeat response: %v\n", msg.response)
			msg.ok <- struct{}{}

		case msg := <-c.reportCh:
			if msg.request.Phase == c.phase {
				log.Printf("Coordinator: Worker has finished %v-task%d\n", c.phase, msg.request.Id)
				c.tasks[msg.request.Id].status = Finished
			}
			msg.ok <- struct{}{}
		}
	}
}

func (c *Coordinator) selectANewTask(response *HeartbeatResponse) bool {
	isAllTaskDone, isNewTaskScheduled := true, false

	for id, task := range c.tasks {
		switch task.status {
		case Idle:
			isAllTaskDone, isNewTaskScheduled = false, true
			c.tasks[id].status, c.tasks[id].startTime = Working, time.Now()
			c.scheduleTaskToResponse(id, response)

		case Working:
			isAllTaskDone = false
			if time.Since(task.startTime) > MaxTaskRunInterval {
				isNewTaskScheduled = true
				c.tasks[id].startTime = time.Now()
				c.scheduleTaskToResponse(id, response)
			}

		case Finished:
		}

		if isNewTaskScheduled {
			break
		}
	}

	if !isNewTaskScheduled && !isAllTaskDone {
		response.JobType = WaitJob
	}

	return isAllTaskDone
}

func (c *Coordinator) scheduleTaskToResponse(taskId int, response *HeartbeatResponse) {
	response.Id = taskId
	switch c.phase {
	case MapPhase:
		response.NReduce = c.nReduce
		response.JobType = MapJob
		response.FilePath = c.tasks[taskId].filePath
	case ReducePhase:
		response.JobType = ReduceJob
		response.NMap = c.nMap
	}
}

func (c *Coordinator) switchPhase() {
	switch c.phase {
	case MapPhase:
		log.Printf("Coordinator: %v is done, start %v\n", c.phase, ReducePhase)
		c.phase = ReducePhase
		c.initReducePhase()
	case ReducePhase:
		log.Printf("Coordinator: %v is done, all tasks are finished\n", c.phase)
		c.phase = CompletePhase
		c.initCompletePhase()
	case CompletePhase:
		log.Printf("Coordinator: Get an unexpected heartbeat in %v\n", c.phase)
	}
}

func (c *Coordinator) selectTaskAfterSwitchPhase(response *HeartbeatResponse) {
	switch c.phase {
	case ReducePhase:
		isAllTaskDone := c.selectANewTask(response)
		if isAllTaskDone {
			c.switchPhase()
			c.selectTaskAfterSwitchPhase(response)
		}
	case CompletePhase:
		response.JobType = CompleteJob
	}
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
	<-c.doneCh
	log.Printf("Coordinator: Done\n")
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		heartbeatCh: make(chan heartbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}, 1),
	}
	go c.schedule()
	c.server()
	return &c
}
