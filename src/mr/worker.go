package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
// hint: to send an RPC to the coordinator asking for a task.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := Args{}
		reply := Reply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if ok {
			// fmt.Printf("%v\n", reply.Message)
			if reply.Task == "MAP" {
				//do map task
				err := mapTask(mapf, reply.Filename, reply.ID, reply.R)
				if err != nil {
					log.Fatal(err)
				}
			} else if reply.Task == "REDUCE" {
				//
				err := reduceTask(reducef, reply.Filenames, reply.ID)
				if err != nil {
					log.Fatal(err)
				}
			} else {
				continue
			}
		} else {
			fmt.Printf("%v\n", reply.Message)
			return
		}
		args = Args{}
		args.ID = reply.ID
		args.Task = reply.Task
		reply = Reply{}
		unfinished := call("Coordinator.Finish", &args, &reply)
		if !unfinished {
			fmt.Printf("%v\n", reply.Message)
			return
		}
	}

}

func mapTask(mapf func(string, string) []KeyValue, filename, id string, intermediate_count int) error {
	//rpc call to get map task
	//open the input file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}
	file.Close()
	//do map func for each keyvalue and sort by key
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	groups := make([][]KeyValue, intermediate_count)
	for i := 0; i < intermediate_count; i++ {
		groups[i] = []KeyValue{}
	}
	//use ihash() for each KeyValue emitted by Map.
	for _, kv := range kva {
		h := ihash(kv.Key) % intermediate_count
		groups[h] = append(groups[h], kv)
	}

	//save the intermediate result
	fliePrefix := "mr-" + id + "-"
	//create  intermediate files
	intermediateFiles, err := createFlies(fliePrefix, intermediate_count)
	if err != nil {
		log.Fatalf("cannot create %v", fliePrefix)
		return err
	}
	saveMapResult(groups, intermediateFiles)
	return nil
}

func createFlies(fliePrefix string, r int) ([]string, error) {
	filenames := []string{}
	for i := 0; i < r; i++ {
		filename := fliePrefix + strconv.Itoa(i) + ".txt"
		filenames = append(filenames, filename)
		file, err := os.Create(filename)
		// fmt.Println(filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
			return nil, err
		}
		defer file.Close()
	}
	return filenames, nil
}

func saveMapResult(groups [][]KeyValue, filenames []string) error {
	n := len(filenames)
	files := make([]*os.File, n)
	for i := 0; i < n; i++ {
		file, err := os.Create(filenames[i])
		files[i] = file
		if err != nil {
			log.Fatalf("cannot create %v", filenames[i])
			return err
		}
		defer files[i].Close()
		enc := json.NewEncoder(files[i])
		for _, kv := range groups[i] {
			err := enc.Encode(&kv)
			if err != nil {
				return err
			}
		}
	}
	return nil

}

func reduceTask(reducef func(string, []string) string, filenames []string, index string) error {
	kva := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return err
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

	}
	sort.Sort(ByKey(kva))
	i := 0
	ofile, err := os.Create("mr-out-" + index + ".txt")
	if err != nil {
		log.Fatalf("cannot create mr-out-" + index)
		return err
	}
	defer ofile.Close()
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

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
