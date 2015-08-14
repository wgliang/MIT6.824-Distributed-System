package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	DPrintf("KillWorkers, worker len: %d \n", len(mr.Workers))
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	doneJobCount := make(chan int)                              // count the jobs done
	undoneJobQueue := make(chan *DoJobArgs, mr.nMap+mr.nReduce) // the queue of undone job
	go func() {
		for {
			worker := <-mr.idleChannel
			args := <-undoneJobQueue
			go func(worker string, doJobArgs *DoJobArgs) {
				var reply DoJobReply
				resp := call(worker, "Worker.DoJob", doJobArgs, &reply)
				if resp {
					mr.idleChannel <- worker
					doneJobCount <- 1
				} else {
					undoneJobQueue <- doJobArgs
				}
			}(worker, args)
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		undoneJobQueue <- &DoJobArgs{mr.file, Map, i, mr.nReduce}
	}

	for i := 0; i < mr.nMap; i++ {
		<-doneJobCount
	}

	for i := 0; i < mr.nReduce; i++ {
		undoneJobQueue <- &DoJobArgs{mr.file, Reduce, i, mr.nMap}
	}

	for i := 0; i < mr.nReduce; i++ {
		<-doneJobCount
	}

	return mr.KillWorkers()
}
