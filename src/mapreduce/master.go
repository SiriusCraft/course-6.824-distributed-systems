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
	var mapChan = make(chan int, mr.nMap)
	var reduceChan = make(chan int, mr.nReduce)

	var send_map = func(worker string, index int) bool {
		var jobArgs DoJobArgs
		var jobReply DoJobReply
		jobArgs.File = mr.file
		jobArgs.Operation = Map
		jobArgs.JobNumber = index
		jobArgs.NumOtherPhase = mr.nReduce
		return call(worker, "Worker.DoJob", jobArgs, &jobReply)
	}

	var send_reduce = func(worker string, index int) bool {
		var jobArgs DoJobArgs
		var jobReply DoJobReply
		jobArgs.File = mr.file
		jobArgs.Operation = Reduce
		jobArgs.JobNumber = index
		jobArgs.NumOtherPhase = mr.nMap
		return call(worker, "Worker.DoJob", jobArgs, &jobReply)
	}

	for i := 0; i < mr.nMap; i++ {
		go func(index int) {
			for {
				var worker string
				var ok bool = false
				select {
				case worker = <-mr.idleChannel:
					ok = send_map(worker, index)
				case worker = <-mr.registerChannel:
					ok = send_map(worker, index)
				}
				if ok {
					mapChan <- index
					mr.idleChannel <- worker
					return
				}
			}
		}(i)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mapChan
	}

	fmt.Println("Map done ...")

	for i := 0; i < mr.nReduce; i++ {
		go func(index int) {
			for {
				var worker string
				var ok bool = false
				select {
				case worker = <-mr.idleChannel:
					ok = send_reduce(worker, index)
				case worker = <-mr.registerChannel:
					ok = send_reduce(worker, index)
				}
				if ok {
					reduceChan <- index
					mr.idleChannel <- worker
					return
				}
			}
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-reduceChan
	}

	fmt.Println("Reduce done ...")

	return mr.KillWorkers()
}
