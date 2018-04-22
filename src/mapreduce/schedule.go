package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	fmt.Printf("%v%v%v%v\n", jobName, mapFiles, phase, n_other)
	//set a waitGroup to wait for map\reduce phase done
	var wg sync.WaitGroup

	//for each task in ntasks
	for i := 0; i < ntasks; i++ {

		//split the tasks into different go routine
		go func(jobName string, file string, phase jobPhase, taskNumber int, numberOfInput int) {
			//add a object to the wg for waiting new task
			wg.Add(1)
			for {
				//get the workers from registerChan
				worker := <-registerChan
				fmt.Printf("Worker received: %s\n", worker)
				//create the Args for call()
				doArgs := DoTaskArgs{JobName: jobName, File: file, Phase: phase, TaskNumber: taskNumber, NumOtherPhase: numberOfInput}
				//call the rpc
				//conf: whether the doarg should be pointer???
				ok := call(worker, "Worker.DoTask", &doArgs, nil)
				// the call func is finish successfully
				if ok {
					// done a waiting group object
					wg.Done()
					//give back the worker to channel
					registerChan <- worker
					break
				} else {
					registerChan <- worker
				}
			}
		}(jobName, mapFiles[i], phase, i, n_other)
	}
	//wait for all the routines done
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
