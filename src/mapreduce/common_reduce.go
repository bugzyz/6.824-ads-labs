package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
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
	// Your code here (Part I).
	//

	//-----------------------load the map-result-files to the keyValues--------
	//open the input file
	fileDecs := make([]*json.Decoder, nMap)
	//for the decode result
	keyValues := make(map[string][]string)
	fmt.Println("\n----------------reduce part sta----------------------")
	fmt.Printf("The configuration is:nMap-%d, reduceTask-%d\n", nMap, reduceTask)
	for i := 0; i < nMap; i++ {
		inputFile, err1 := os.Open(reduceName(jobName, i, reduceTask))
		if err1 != nil {
			fmt.Println(err1)
		} else {
			fmt.Printf("successfully open %s\n", reduceName(jobName, i, reduceTask))
		}

		//decode the input content to struct
		fileDecs[i] = json.NewDecoder(inputFile)
		//decode contents from inputfile
		for ii := 0; ; ii++ {
			//make a temp KeyValue to store intermidiate data
			var kv KeyValue
			ok := fileDecs[i].Decode(&kv)

			if ok == nil {
				//if the keyValues didn;t exist a key
				if _, okk := keyValues[kv.Key]; !okk {
					keyValues[kv.Key] = make([]string, 0)
				}
				//append the values to keyValues[kv.Key]
				keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
			} else {
				//finish loading all the content from inputfile x (there are nMap inputfiles)
				fmt.Printf("finish!	successfully load %s\n", reduceName(jobName, i, reduceTask))
				break
			}
		}
		//close the input file
		inputFile.Close()
	}

	//-------------------traverse the keyValues to run the reduceF()----------
	keys := make([]string, 0)
	//transfer the keys to string[] "keys" to sort the keys
	for k := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	outputfile, err := os.Create(mergeName(jobName, reduceTask))
	if err == nil {
		encod := json.NewEncoder(outputfile)
		for _, k := range keys {
			encod.Encode(&KeyValue{k, reduceF(k, keyValues[k])})
		}
	} else {
		fmt.Printf("failed to create the reduce output file:%s\n", mergeName(jobName, reduceTask))
	}
	outputfile.Close()

}
