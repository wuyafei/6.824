package mapreduce

import "container/list"
import "fmt"
import "log"


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
	// Your code here
	jobDoneChannel := make(chan bool)
	for i := 0; i<mr.nMap; i++{
		go func (mapJobNumber int) {
			for{
				worker := <-mr.registerChannel
				args := &DoJobArgs{mr.file, Map, mapJobNumber, mr.nReduce}
				reply := &DoJobReply{}
				jobOK := call(worker,"Worker.DoJob",args,reply)
				if(jobOK){
					jobDoneChannel <- true
					mr.registerChannel <- worker
					return
				}
			}
		}(i)
	}
	for i := 0; i<mr.nMap; i++{
		<-jobDoneChannel
		log.Printf("map job %d is done",i)
	}
	log.Printf("map job done")
	for i := 0; i<mr.nReduce; i++{
		go func (reduceJobNumber int) {
			for{
				worker := <-mr.registerChannel
				args := &DoJobArgs{mr.file, Reduce, reduceJobNumber, mr.nMap}
				reply := &DoJobReply{}
				jobOK := call(worker,"Worker.DoJob",args,reply)
				if(jobOK){
					jobDoneChannel <- true
					mr.registerChannel <- worker
					return
				}
			}
		}(i)
	}
	for i := 0; i<mr.nReduce; i++{
		<-jobDoneChannel
		log.Printf("reduce job %d is done",i)
	}
	log.Printf("reduce job done")

	return mr.KillWorkers()
}
