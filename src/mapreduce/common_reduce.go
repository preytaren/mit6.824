package mapreduce

import (
	"encoding/json"
	"os"
	"log"
	//"fmt"
)
// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	var tmp KeyValue
	data := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		f, err := os.OpenFile(fileName, os.O_RDWR, 0755)
		//fmt.Println(outFile)
		if err != nil {
			log.Fatal(err)
		}
		decoder := json.NewDecoder(f)
		for {
			err := decoder.Decode(&tmp)
			if err != nil {
				break
			}
			data[tmp.Key] = append(data[tmp.Key], tmp.Value)
			//fmt.Println("key" ,data[tmp.Key])
		}
	}

	file, _ := os.OpenFile(outFile, os.O_RDWR | os.O_CREATE, 0755)
	enc := json.NewEncoder(file)
	for key, value := range data {
	    enc.Encode(&KeyValue{key, reduceF(key, value)})
	}
	file.Close()

}
