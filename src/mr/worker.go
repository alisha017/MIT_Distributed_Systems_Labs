package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallExample()
	assignedId := CallGetWorkerId()
	for {
		taskCompleted := CallAskForTask(assignedId, mapf, reducef)
		fmt.Println("/n____Task for worker", assignedId, "processed: ", taskCompleted.Task, "_______/n")

		if taskCompleted.Task == DONE {
			// Notify the coordinator that the worker is shutting down
			err := CallWorkerShutdown(assignedId)
			if err != nil {
				log.Printf("Failed to notify coordinator of worker shutdown: %v", err)
			}
			break
		} else if taskCompleted.Task == WAITING {
			time.Sleep(5 * time.Second)
			continue
		}
		completed, err := CallReportTaskCompleted(taskCompleted)
		if err != nil {
			fmt.Println("Error in reporting completed task:", err)
		} else {
			fmt.Println("Worker", assignedId, "task acknowledged:", completed.Acknowledged)
		}
		time.Sleep(2 * time.Second)
	}

	time.Sleep(2 * time.Second)
}

// CallExample - example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// CallGetWorkerId - first call to the coordinator to fetch the worker id
func CallGetWorkerId() string {
	// Assuming you need an args structure, even if it's empty
	args := GetWorkerIdArgs{} // empty struct, modify based on actual requirements
	reply := GetWorkerIdReply{}
	ok := call("Coordinator.GetWorkerId", &args, &reply)

	if ok {
		fmt.Printf("Assigned worker ID = %v\n", reply.WorkerId)
		return reply.WorkerId
	} else {
		fmt.Printf("call failed!\n")
		return ""
	}

}

// CallAskForTask - worker calling coordinator to be assigned tasks
func CallAskForTask(id string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) TaskCompleteArgs {

	args := AskForTaskArgs{
		WorkerId: id,
	}
	completedArgs := TaskCompleteArgs{
		WorkerID: args.WorkerId,
		Success:  false,
	}
	reply := AskForTaskReply{}
	fmt.Println("*************Asking for task in Worker:", id, "****************")
	ok := call("Coordinator.AskForTask", &args, &reply)
	if ok {
		//fmt.Println("In ask for task, got reply:", reply)
		filenames := reply.FileLoc
		completedArgs.Task = reply.Task
		completedArgs.FileProcessed = filenames
		completedArgs.RTask = reply.RTask
		//fmt.Printf("reply.FileLoc %v\nreply.nReduce: %v\n", filenames, reply.NReduce)

		// read the input file, pass it to Map and accumulate intermediate Map output
		switch reply.Task {
		case MAP:
			{
				completedArgs.OutputFiles = processMap(filenames[0], mapf, reply.NReduce, reply.NMap)
				completedArgs.Success = true
			}
		case REDUCE:
			{
				completedArgs.OutputFiles = processReduce(filenames, reducef, reply.RTask)
				completedArgs.Success = true
			}
		case DONE:
			fmt.Println("Yayy!! All tasks done!")
		case WAITING:
			fmt.Println("------> Waiting for coordinator to assign task...")
		default:
			fmt.Println("Wrong task assignment")
		}
	} else {
		fmt.Println("Call to Coordinator.AskForTask failed!")
	}

	fmt.Println("From ask for task, returning :", completedArgs.Task.String())
	return completedArgs
}

// CallReportTaskCompleted -  after the assigned task is completed,
func CallReportTaskCompleted(completedTask TaskCompleteArgs) (*TaskCompleteReply, error) {
	args := completedTask
	reply := TaskCompleteReply{}
	fmt.Println("-----------------Reporting task completed-----------------")
	if ok := call("Coordinator.ReportTaskCompleted", &args, &reply); ok {
		return &reply, nil
	}
	return nil, fmt.Errorf("failed to report task completion")
}

// CallWorkerShutdown - function to notify coordinator that worker is shutting down
func CallWorkerShutdown(workerId string) error {
	args := WorkerShutdownArgs{
		WorkerID: workerId,
	}
	reply := WorkerShutdownReply{}

	ok := call("Coordinator.WorkerShutdown", &args, &reply)
	if ok && reply.Acknowledged {
		fmt.Printf("Worker %s shutdown acknowledged by coordinator\n", workerId)
		return nil
	} else {
		return fmt.Errorf("Failed to notify coordinator of worker shutdown")
	}
}

func processMap(fileLoc string, mapf func(string, string) []KeyValue, nReduce int, id int) []string {
	fmt.Println("........Processing MAP function for file:", fileLoc, "...........")

	file, err := os.Open(fileLoc)
	if err != nil {
		log.Fatalf("cannot open %v", fileLoc)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileLoc)
	}
	defer file.Close() // It's important to close the file when you're finished.

	intermediate := []KeyValue{}
	kva := mapf(fileLoc, string(content))
	intermediate = append(intermediate, kva...)

	// Open files and create encoders
	encoders := make([]*json.Encoder, nReduce)
	outputFiles := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("/Users/alisha/Desktop/Study/System_Design/MITLabs/6.5840/src/mr/temp/mr_%v_%d", id, i+1)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("failed to create file: %s", err)
		}
		defer file.Close()
		encoders[i] = json.NewEncoder(file)
		outputFiles[i] = filename
	}

	// Assuming intermediate contains the data
	//for i := 0; i < len(intermediate) && i < 5; i++ {
	for _, kv := range intermediate {
		index := ihash(kv.Key) % nReduce
		//fmt.Println(kv.Key, kv.Value, index)
		err := encoders[index].Encode(&kv)
		if err != nil {
			log.Fatalf("failed to encode kv: %s", err)
		}
	}

	return outputFiles
}

func processReduce(fileLocs []string, reducef func(string, []string) string, rTask int) []string {

	fmt.Println("````````````Processing reduce: ", rTask, "````````````````")
	var kva []KeyValue
	for _, fileLoc := range fileLocs {
		file, err := os.Open(fileLoc)
		if err != nil {
			fmt.Errorf("Error opening file %s, %v", fileLoc, err)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break // End of file reached
				}
				fmt.Errorf("error decoding JSON from file %s: %v", fileLoc, err)
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	outName := "mr-out-" + strconv.Itoa(rTask)
	outFile, _ := os.Create(outName)
	defer outFile.Close()
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	return []string{outName}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(".....Error in RPC:", err)
	return false
}
