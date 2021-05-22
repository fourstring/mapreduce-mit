package mapreduce

import (
	"fmt"
	"sync/atomic"
)

type WaitChan struct {
	tasks int32
	done  chan struct{}
}

func NewWaitChan() *WaitChan {
	return &WaitChan{tasks: 0, done: make(chan struct{})}
}

func (w *WaitChan) Add() {
	atomic.AddInt32(&w.tasks, 1)
	// fmt.Printf("Remain Tasks: %d\n", res)
}

func (w *WaitChan) DoneChan() chan struct{} {
	return w.done
}

func (w *WaitChan) Done() {
	res := atomic.AddInt32(&w.tasks, -1)
	// fmt.Printf("Remain Tasks: %d\n", res)
	if res == 0 {
		w.done <- struct{}{}
		// fmt.Printf("Sent done notify")
	}
}

func (w *WaitChan) Close() {
	close(w.done)
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
type Task struct {
	args      DoTaskArgs
	reclaimed bool
}

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
	reclaimChan := make(chan string, 8)
	taskChan := make(chan Task, ntasks)
	defer close(reclaimChan)
	defer close(taskChan)

	for i := 0; i < ntasks; i++ {
		taskChan <- Task{
			args: DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[i],
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: n_other,
			},
			reclaimed: false,
		}
	}
	waitChan := NewWaitChan()
	done := false

	for !done {
		select {
		case task := <-taskChan:
			if !task.reclaimed {
				waitChan.Add()
			}
			var worker string
			select {
			case w := <-registerChan:
				// fmt.Printf("given worker %s %v task #%d/%d, from registration\n", w, phase, task.args.TaskNumber, ntasks)
				worker = w
			case w := <-reclaimChan:
				// fmt.Printf("given worker %s %v task #%d/%d, from reclaim\n", w, phase, task.args.TaskNumber, ntasks)
				worker = w
			}
			go func() {
				var _r struct{}
				success := call(worker, "Worker.DoTask", task.args, &_r)
				if success {
					waitChan.Done()
					reclaimChan <- worker
				} else {
					taskChan <- Task{
						args:      task.args,
						reclaimed: true,
					}
				}
			}()
		case <-waitChan.DoneChan():
			done = true
		}
	}

	fmt.Printf("Schedule: %v waitChan\n", phase)
}
