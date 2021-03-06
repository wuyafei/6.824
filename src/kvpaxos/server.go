package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation       string //get, put, append
	Key             string
	Value           string
	UUID            int64          //indicate request itself
	ID              int64          //indicate the client send this request
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	db        map[string]string      //database to store key-value pair
	uuid      map[int64]int64        //client_id => last request uuid, at-most-once
	processed_seq   int              //paxos peers processed seq number
}

func (kv *KVPaxos) ApplySeq(seq int, op Op){
		if op.Operation == "Put"{
			kv.db[op.Key] = op.Value
			//fmt.Printf("Put %v:%v\n",op.Key, op.Value)
		}else if op.Operation == "Append"{
			kv.db[op.Key] = kv.db[op.Key] + op.Value
			//if op.Value=="0" || op.Value=="1" || op.Value=="2"{
			//	fmt.Printf("cli:%v, uuid: %v, key: %v Value: %v\n",op.ID, op.UUID, op.Key, op.Value)
			//}
		}
		kv.uuid[op.ID] = op.UUID
		kv.processed_seq=seq
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//we don't need to consider the at-most-onec problem in Get
	//we need to start a paxos instance for this request
	op := Op{"Get", args.Key, "", args.UUID, args.ID}
	kv.px.Start(kv.processed_seq+1,op)
	for {
		status, val := kv.px.Status(kv.processed_seq+1)
		if(status == paxos.Pending){
			time.Sleep(100*time.Millisecond)
		}else if args.UUID != val.(Op).UUID{
			kv.ApplySeq(kv.processed_seq+1, val.(Op))
			kv.px.Start(kv.processed_seq+1,op)
		}else{
			value, exists := kv.db[args.Key]
			if exists {
				reply.Err = OK
			}else{
				reply.Err = ErrNoKey
			}
			reply.Value = value
			kv.uuid[op.ID] = op.UUID
			kv.processed_seq++
			break
		}
	}
	kv.px.Done(kv.processed_seq)
	kv.px.Min()
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.uuid[args.ID] >= args.UUID{
		return nil
	}
	op := Op{args.Op, args.Key, args.Value, args.UUID, args.ID}
	kv.px.Start(kv.processed_seq+1,op)
	for {
		status, val := kv.px.Status(kv.processed_seq+1)
		if status == paxos.Pending{
			time.Sleep(100*time.Millisecond)
		}else if args.UUID != val.(Op).UUID{
			kv.ApplySeq(kv.processed_seq+1, val.(Op))
			kv.px.Start(kv.processed_seq+1,op)
		}else{
			kv.ApplySeq(kv.processed_seq+1, val.(Op))
			reply.Err = OK
			break
		}
	}
	kv.px.Done(kv.processed_seq)
	kv.px.Min()
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.processed_seq = 0
	kv.db = make(map[string]string)
	kv.uuid = make(map[int64]int64)

	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
