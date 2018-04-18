package mapreduce

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string, //mine: input file name
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	//
	// doMap manages one map task: it should read one of the input files
	// (inFile), call the user-defined map function (mapF) for that file's
	// contents, and partition mapF's output into nReduce intermediate files.
	//
	// There is one intermediate file per reduce task. The file name
	// includes both the map task number and the reduce task number. Use
	// the filename generated by reduceName(jobName, mapTask, r)
	// as the intermediate file for reduce task r. Call ihash() (see
	// below) on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//
	// Your code here (Part I).
	//
	fmt.Print(inFile)

	//------------read file s----------------------
	//open file
	file, err := os.Open(inFile)
	if err != nil {
		fmt.Print(err)
	} else {
		fmt.Print("open file success!")
	}
	//get the length of file
	fi, _ := file.Stat()
	contents := make([]byte, fi.Size())
	//load content to contents
	file.Read(contents)
	file.Close()
	//------------read file e----------------------

	//------------create reduce files s------------
	kv := mapF(inFile, string(contents))
	filesEnc := make([](*json.Encoder), nReduce)
	files := make([]*os.File, nReduce)

	//set the name of the reduce-input-file
	for i := range filesEnc {
		f, err1 := os.Create(reduceName(jobName, mapTask, i))
		if err1 == nil {
			//set the target file info to the encoder
			filesEnc[i] = json.NewEncoder(f)
			//store the reduce-file-info to the files
			files[i] = f
		} else {
			fmt.Print(err1)
		}
	}

	//write the map result to the reduce-input-file
	for _, v := range kv {
		//write the value of the map result to the encoder
		err2 := filesEnc[ihash(v.Key)%nReduce].Encode(&v)
		if err2 != nil {
			fmt.Print(err2)
		}
	}

	//close the files that opened to write reduce-input-data
	for _, f := range files {
		f.Close()
	}

}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
