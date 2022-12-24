package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const (
	restPeriod = 1 * time.Second
)

var (
	intFilename = "mr-%d-%d"
	outFilename = "mr-out-%d"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func genWorkerNum() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(100)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerID := genWorkerNum()

	var err error
	for err == nil {
		err = CallGimmeWork(workerID, mapf, reducef)
		time.Sleep(restPeriod)
	}
	log.Fatal(err)
}

func CallGimmeWork(workerID int, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {
	args := WorkArgs{
		WorkerID: workerID,
	}

	reply := &TaskReply{}

	ok := call("Coordinator.GimmeWork", &args, reply)
	if !ok {
		return fmt.Errorf("no work to be done")
	}

	fmt.Printf("reply.filename %v\n", reply.Filename)
	fmt.Printf("reply.workType %v\n", reply.TaskType)
	if err := doWork(reply, mapf, reducef); err != nil {
		return err
	}

	return nil
}

func CallTaskDone(filename string, taskType workType) error {
	var rpcFunc string
	switch taskType {
	case MapTask:
		rpcFunc = "Coordinator.MapTaskDone"
	case ReduceTask:
		rpcFunc = "Coordinator.ReduceTaskDone"
	default:
		return fmt.Errorf("unknown task type: %s", taskType)
	}

	args := TaskCompletedArgs{
		TaskKey:  filename,
		TaskType: taskType,
	}

	reply := &TaskCompletedReply{}

	ok := call(rpcFunc, &args, reply)
	if ok {
		fmt.Printf("reply.comfirmed %v\n", reply.Confirmed)
	} else {
		return fmt.Errorf("no work to be done")
	}
	if reply.Confirmed {
		fmt.Printf("%s task %s is completed\n", taskType, filename)
		return nil
	}
	return fmt.Errorf("task %s is not completed", filename)
}

func doWork(task *TaskReply, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) error {
	switch task.TaskType {
	case MapTask:
		if err := doMap(task, mapf); err != nil {
			return fmt.Errorf("map task failed: %v\n", err)
		}
	case ReduceTask:
		if err := doReduce(task, reducef); err != nil {
			return fmt.Errorf("reduce task failed: %v\n", err)
		}
	default:
		return fmt.Errorf("unknown task: %s", task.TaskType)
	}
	return nil
}

func doMap(reply *TaskReply, mapf func(string, string) []KeyValue) error {
	fmt.Printf("executing map task '%s'...\n", reply.Filename)
	// time.Sleep(time.Second * 12)
	// fmt.Println("sleeping is over")
	content, err := readFile(reply.Filename)
	if err != nil {
		log.Fatal(err)
	}

	res := mapf(reply.Filename, string(content))
	splittedContent := make([][]KeyValue, reply.NReduce)
	for _, kv := range res {
		reduceNum := ihash(kv.Key) % reply.NReduce
		splittedContent[reduceNum] = append(splittedContent[reduceNum], kv)
	}

	for fileNum, kvSlice := range splittedContent {
		filename := fmt.Sprintf(intFilename, reply.TaskID, fileNum)
		if err := writeFile(filename, kvSlice); err != nil {
			log.Fatal(err)
		}
	}

	return CallTaskDone(reply.Filename, MapTask)
}

func doReduce(task *TaskReply, reducef func(string, []string) string) error {
	// time.Sleep(time.Second * 12)
	// fmt.Println("sleeping is over")
	oFilename := fmt.Sprintf(outFilename, task.TaskID)

	intFiles := make([]string, task.MapsNum)
	for i := 0; i < task.MapsNum; i++ {
		intFiles[i] = fmt.Sprintf(intFilename, i, task.TaskID)
	}

	ofile, _ := os.Create(oFilename)
	var fullKVContent []KeyValue
	for _, intFile := range intFiles {
		fmt.Printf("executing reduce task '%s'...\n", intFile)
		content, err := readFile(intFile)
		if err != nil {
			log.Fatal(err)
		}

		var kvContent []KeyValue
		if err := json.Unmarshal(content, &kvContent); err != nil {
			log.Fatal(err)
		}
		fullKVContent = append(fullKVContent, kvContent...)
	}

	sort.Sort(ByKey(fullKVContent))

	i := 0
	for i < len(fullKVContent) {
		j := i + 1
		for j < len(fullKVContent) && fullKVContent[j].Key == fullKVContent[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, fullKVContent[k].Value)
		}
		output := reducef(fullKVContent[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", fullKVContent[i].Key, output)
		i = j
	}
	ofile.Close()
	return CallTaskDone(task.Filename, ReduceTask)
}

func readFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Printf("cannot open file %v: %v\n", filename, err)
		return nil, err
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read file %v: %v\n", filename, err)
		return nil, err
	}
	return content, nil
}

func writeFile(filename string, content []KeyValue) error {
	outFile, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("cannot write to file %s: %v", filename, err)
	}

	bytes, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("cannot marshal content of file %s: %v", filename, err)
	}

	outFile.Write(bytes)
	return nil
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

	fmt.Println(err)
	return false
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}
