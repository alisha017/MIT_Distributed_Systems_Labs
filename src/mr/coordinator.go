package mr

import (
	"fmt"
	"github.com/golang-collections/collections/queue"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

// WorkerState defines possible states of a worker.
type WorkerState int

const (
	Connected WorkerState = iota + 1 // start from 1
	Unavailable
	Disconnected
)

func (t WorkerState) String() string {
	switch t {
	case Connected:
		return "Connected"
	case Unavailable:
		return "Unavailable"
	case Disconnected:
		return "Disconnected"
	default:
		return "Unknown Worker State"
	}
}

// FileStatus represents the status of a file in the processing workflow.
type FileStatus int

const (
	NotProcessed FileStatus = iota
	Processing
	Processed
)

func (t FileStatus) String() string {
	switch t {
	case Processed:
		return "Processed"
	case NotProcessed:
		return "Not Processed"
	case Processing:
		return "In Progress"
	default:
		return "Unknown File processing status"
	}
}

type Coordinator struct {
	// Your definitions here.

	muLock           sync.Mutex
	ConnectedWorkers map[string]WorkerState // Set of workers using map as a set
	TotalWorkers     int
	nReduce          int
	nMap             int
	AllFiles         map[string]int

	MapTaskQueue     *queue.Queue       // queue of files to be processed on MAP function
	MapTaskStatus    map[int]FileStatus // New map to track file statuses
	MapOutputFiles   map[int][]string   // temp files created by map task {reduce task : list of files}
	MapTaskCompleted int

	ReduceTaskQueue     *queue.Queue       // queue to assign reduce task to workers
	ReduceTaskStatus    map[int]FileStatus // map for if the reduce task is processed or not
	ReduceOutputFiles   map[int]string     // location for each reduce task {reduce task: output location}
	ReduceTaskCompleted int

	TaskCompletionChans map[string]chan bool // Channels to signal task completion, keyed by task ID
}

// GetWorkerId when worker connects with coordinator, assign worker_id
// maintain a global variable which contains worker ids which are connecting with the coordinator
// if worker id is not connected, remove from this data structure -> set or hashmap?
func (c *Coordinator) GetWorkerId(args *GetWorkerIdArgs, reply *GetWorkerIdReply) error {
	c.muLock.Lock()         // Lock to ensure that incrementing TotalWorkers is thread-safe
	defer c.muLock.Unlock() // Unlock after modifications are done
	c.TotalWorkers++
	newId := fmt.Sprintf("%d", c.TotalWorkers) // Convert the incremented counter to a string
	reply.WorkerId = newId                     // Assign the new ID to the reply
	c.ConnectedWorkers[newId] = Connected      // Mark this worker as connected
	//fmt.Printf("Connected workers: %v", c.ConnectedWorkers)
	return nil
}

// AskForTask to serve worker's request for task.
func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	// Print the received ID to the server's log.
	log.Println("\n***************New task!!!*********************")
	log.Printf("Received task request with ID: %s", args.WorkerId)
	log.Println("Connected Workers:", c.ConnectedWorkers)

	// Lock the mutex before accessing the FilesLocList
	c.muLock.Lock()
	defer c.muLock.Unlock()

	// Check if all tasks are completed
	allTasksDone := (c.MapTaskCompleted >= c.nMap && c.ReduceTaskCompleted >= c.nReduce)

	if allTasksDone {
		reply.Task = DONE
		log.Println("\n>>>>>All tasks have been completed. Sending DONE status.<<<<<<")
		return nil
	}

	reply.NReduce = c.nReduce
	// if queue has elements, means the files are not allocated to any worker, hence assign the FileLoc to the worker
	// the if condition inside makes sure that the reply.FileLoc is a string, type assertion
	if c.MapTaskCompleted < c.nMap {
		if c.MapTaskQueue.Len() > 0 {
			item := c.MapTaskQueue.Dequeue()
			if fileLoc, ok := item.(string); ok {
				reply.Task = MAP
				reply.FileLoc = []string{fileLoc}
				reply.RTask = 0
				reply.NMap = c.AllFiles[fileLoc]
				c.MapTaskStatus[c.AllFiles[fileLoc]] = Processing // Update the status to processing
				log.Println("Map Task Status:", c.MapTaskStatus)
				// Start a goroutine to handle timeout
				taskId := generateTaskIdForWorker(args.WorkerId, reply.Task.String(), fileLoc)
				go c.handleTaskTimeout(taskId)
				return nil
			} else {
				// Handle the error if the item is not a string
				log.Printf("Error: Expected a string in map queue, got %T", item)
				return fmt.Errorf("type assertion to string failed for queue item")
			}
		}
	}

	if c.ReduceTaskCompleted < c.nReduce {
		/*
			get the available reduce task from the queue
			fetch the list of files to be processed by reduce from mapoutputfiles
			update the reducetask to processing in the map
		*/
		if c.ReduceTaskQueue.Len() > 0 {
			item := c.ReduceTaskQueue.Dequeue()
			if fileList, ok := c.MapOutputFiles[item.(int)]; ok {
				reply.Task = REDUCE
				reply.FileLoc = fileList
				reply.RTask = item.(int)
				c.ReduceTaskStatus[item.(int)] = Processing
				log.Println("Reduce task Status:", c.ReduceTaskStatus)
				taskId := generateTaskIdForWorker(args.WorkerId, reply.Task.String(), strconv.Itoa(reply.RTask))
				go c.handleTaskTimeout(taskId)
				return nil
			} // Handle the error if the item is not a string
			log.Printf("Error: Expected a int in reduce queue, got %T", item)
			return fmt.Errorf("type assertion to int failed for queue item")
		}

	}

	reply.Task = WAITING
	log.Println(".....No tasks available at the moment. Sending WAITING status....")
	return nil
}

func (c *Coordinator) ReportTaskCompleted(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()
	fmt.Println("\n------------Reporting task completed for: ", args.Task, args.Success, "----------------")
	var taskId string

	// Record the output files, update the task status, etc.
	// if map is successful, add the reducetask to the reducequeue and add to mapoutfiles
	if args.Task == MAP {
		taskId = generateTaskIdForWorker(args.WorkerID, args.Task.String(), args.FileProcessed[0])
		if args.Success {
			for i, file := range args.OutputFiles {
				//parts := strings.Split(file, "_")
				//reduceNum, err := strconv.Atoi(parts[len(parts)-1])
				reduceNum := i + 1
				//if err != nil {
				//	log.Fatalf("Map file output naming wrong: should be - mr_<workerid>_<reducenum>")
				//} else {
				c.MapOutputFiles[reduceNum] = append(c.MapOutputFiles[reduceNum], file)
				c.ReduceTaskQueue.Enqueue(reduceNum)
				c.ReduceTaskStatus[reduceNum] = NotProcessed
				//}
			}
			//fmt.Println("******Files processed:", args.FileProcessed)
			c.MapTaskStatus[c.AllFiles[args.FileProcessed[0]]] = Processed
			c.MapTaskCompleted += 1
		} else {
			fmt.Println("******....Files not processed:", args.FileProcessed)
			c.requeueMapTask(args.FileProcessed[0])
			//c.MapTaskStatus[c.AllFiles[args.FileProcessed[0]]] = NotProcessed
			//c.MapTaskQueue.Enqueue(args.FileProcessed)
		}
	} else if args.Task == REDUCE {
		taskId = generateTaskIdForWorker(args.WorkerID, args.Task.String(), strconv.Itoa(args.RTask))
		if args.Success {
			c.ReduceOutputFiles[args.RTask] = args.OutputFiles[0]
			c.ReduceTaskStatus[args.RTask] = Processed
			c.ReduceTaskCompleted += 1
		} else {
			fmt.Println("******....Files not processed for reduce task:", args.RTask)
			c.requeueReduceTask(args.RTask)
			//c.ReduceTaskStatus[args.RTask] = NotProcessed
			//c.ReduceTaskQueue.Enqueue(args.RTask)
		}
	}

	// Signal task completion on the corresponding channel if it exists
	if ch, ok := c.TaskCompletionChans[taskId]; ok {
		ch <- true
		close(ch)                             // Optionally close the channel if no further signals are expected
		delete(c.TaskCompletionChans, taskId) // Clean up the channel from the map
	}
	reply.Acknowledged = true
	return nil
}

// WorkerShutdown handles the worker shutdown and marks the worker as Disconnected
func (c *Coordinator) WorkerShutdown(args *WorkerShutdownArgs, reply *WorkerShutdownReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	// Update the worker's state to Disconnected
	if _, ok := c.ConnectedWorkers[args.WorkerID]; ok {
		c.ConnectedWorkers[args.WorkerID] = Disconnected
		log.Printf("Worker %s has shut down and is marked as Disconnected", args.WorkerID)
		reply.Acknowledged = true
	} else {
		log.Printf("Worker %s not found, unable to mark as Disconnected", args.WorkerID)
		reply.Acknowledged = false
	}
	return nil
}

// Example an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// handleTaskTimeout handles the timeout for a task assigned to a worker.
func (c *Coordinator) handleTaskTimeout(taskID string) {
	// Create a channel for this specific task
	completionChan := make(chan bool)
	c.TaskCompletionChans[taskID] = completionChan

	select {
	case <-completionChan:
		// Task completed successfully before timeout
		log.Println("Task completed before timeout:", taskID)
	case <-time.After(10 * time.Second):
		// Timeout occurred
		c.muLock.Lock()
		defer c.muLock.Unlock()
		fmt.Println("Took longer than 10s, requeueing", taskID)
		c.requeueTask(taskID) // Implement this to requeue the task
		if _, ok := c.ConnectedWorkers[taskID]; ok {
			c.ConnectedWorkers[taskID] = Unavailable
		}
		log.Printf("Task timeout for taskID %d, task requeued", taskID)
	}
}

func generateTaskIdForWorker(workerId, task, task_q string) string {
	return workerId + "_" + task + "_" + task_q
}

// requeueTask uses the taskID to determine whether to requeue a map or reduce task.
func (c *Coordinator) requeueTask(taskID string) {
	parts := strings.Split(taskID, "_")
	if len(parts) != 3 {
		fmt.Println("Invalid taskID format:", taskID)
		return
	}

	workerID := parts[0]
	taskType := parts[1]
	taskQualifier := parts[2] // This is either the file name for map tasks or the reduce task number for reduce tasks.

	switch taskType {
	case "MAP":
		c.requeueMapTask(taskQualifier)
	case "REDUCE":
		reduceTaskNum, err := strconv.Atoi(taskQualifier)
		if err != nil {
			fmt.Println("Error converting reduce task number:", err)
			return
		}
		c.requeueReduceTask(reduceTaskNum)
	default:
		fmt.Println("Unknown task type:", taskType)
	}

	// Optionally update the worker status to reflect the requeue
	c.muLock.Lock()
	c.ConnectedWorkers[workerID] = Unavailable // Assuming worker becomes unavailable if task needs requeuing
	c.muLock.Unlock()
}

// Requeue a map task by file name
func (c *Coordinator) requeueMapTask(fileName string) {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	c.MapTaskQueue.Enqueue(fileName)
	fileID := c.AllFiles[fileName] // Assuming AllFiles maps file names to their respective task IDs
	c.MapTaskStatus[fileID] = NotProcessed
}

// Requeue a reduce task by task number
func (c *Coordinator) requeueReduceTask(reduceTaskNum int) {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	c.ReduceTaskQueue.Enqueue(reduceTaskNum)
	c.ReduceTaskStatus[reduceTaskNum] = NotProcessed
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	// Check if all Map and Reduce tasks are completed
	allTasksCompleted := c.MapTaskCompleted >= c.nMap && c.ReduceTaskCompleted >= c.nReduce

	if allTasksCompleted {
		// Additional check: make sure all workers have received the DONE status
		allWorkersAcknowledged := true
		for _, state := range c.ConnectedWorkers {
			if state == Connected {
				allWorkersAcknowledged = false
				break
			}
		}

		if allWorkersAcknowledged {
			log.Println(c.ConnectedWorkers)
			log.Println(">>>>All tasks completed and all workers acknowledged DONE status. Shutting down coordinator.<<<<")
			return true
		}

		// If not all workers have acknowledged, don't shutdown yet
		log.Println("~~~~All tasks completed, but waiting for workers to acknowledge DONE status.~~~~")
	}

	return false
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.ConnectedWorkers = make(map[string]WorkerState) // Properly initialize the map
	c.TotalWorkers = 0
	c.nMap = 0
	c.nReduce = nReduce

	c.MapTaskCompleted = 0
	c.ReduceTaskCompleted = 0
	c.AllFiles = make(map[string]int)
	c.MapTaskQueue = queue.New()
	c.MapTaskStatus = make(map[int]FileStatus)
	c.ReduceTaskQueue = queue.New()
	c.ReduceTaskStatus = make(map[int]FileStatus)

	c.MapOutputFiles = make(map[int][]string)
	c.ReduceOutputFiles = make(map[int]string)

	c.TaskCompletionChans = make(map[string]chan bool)

	for i, file := range files {
		c.MapTaskQueue.Enqueue(file)
		c.MapTaskStatus[i] = NotProcessed
		c.nMap += 1
		c.AllFiles[file] = c.nMap
	}

	c.server()
	return &c
}
