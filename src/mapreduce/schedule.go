package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	var barrier sync.WaitGroup

	for i:=0; i<ntasks; i++ {
		barrier.Add(1)
		go func(taskNum int) {
			defer barrier.Done()
			worker := <-mr.registerChannel

			var args DoTaskArgs
			args.JobName = mr.jobName
			args.File = mr.files[taskNum]
			args.Phase = phase
			args.TaskNumber = taskNum
			args.NumOtherPhase = nios
			ok := call(worker, "Worker.DoTask", args, new(struct{}))
			if ok {
				go func() {
					mr.registerChannel <- worker
				}()
			}
		}(i)

	}

	barrier.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)
}
