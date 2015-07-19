package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	Operation    string
	Key          string
	Value        string
	UUID         int64
	ID           int64
	Config       shardmaster.Config
	ReconfigContent    PullShardReply
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	config    shardmaster.Config
	db        map[string]string
	uuid      map[int64]int64
	processed_seq    int
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//we don't need to consider the at-most-onec problem in Get
	//we need to start a paxos instance for this request
	op := Op{Operation: "Get", Key: args.Key, UUID: args.UUID, ID: args.ID}
	reply.Err, reply.Value = kv.LogOp(op)
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{Operation: args.Op, Key: args.Key, Value: args.Value, UUID: args.UUID, ID: args.ID}
	reply.Err, _ = kv.LogOp(op)
	return nil
}

func (kv *ShardKV) ApplyGet(op Op) (Err, string){
	var err Err
	var value string
	
	val, exists := kv.db[op.Key]
	if exists {
		err = OK
		value = val
	}else{
		err = ErrNoKey
	}
	return err, value
}

func (kv *ShardKV) ApplyPutAppend(op Op) (Err, string) {
	var err Err
	var value string
	
	if op.Operation == "Put"{
		kv.db[op.Key] = op.Value
	}else if op.Operation == "Append"{
		kv.db[op.Key] = kv.db[op.Key] + op.Value
	}
	err = OK
	return err, value
}

func (kv *ShardKV) ApplyReconfig(op Op) (Err, string) {
	var err Err
	var value string
	content := &op.ReconfigContent
	for k, v := range content.DB {
		kv.db[k] = v
	}
	for k, v := range content.UUID {
		uuid, exists := kv.uuid[k]
		if !exists || uuid < v {
			kv.uuid[k] = v
		}
	}
	kv.config = op.Config
	return err, value
}

func (kv *ShardKV) ApplyLog(seq int, op Op) (Err, string){
	var err Err
	var value string
	switch op.Operation {
	case "Get":
		err, value = kv.ApplyGet(op)
		kv.uuid[op.ID] = op.UUID
	case "Put", "Append":
		err, value = kv.ApplyPutAppend(op)
		kv.uuid[op.ID] = op.UUID
	case "Reconfig":
		err, value = kv.ApplyReconfig(op)
	case "PullShard":

	}
	kv.processed_seq = seq
	return err, value
}

func (kv *ShardKV) LogOp(op Op) (Err, string){
	var err Err
	var value string
	switch op.Operation{
	case "Reconfig":
		if kv.config.Num >= op.Config.Num{
			err = OK
		}
	case "Put", "Append":
		shard := key2shard(op.Key)
		if kv.gid != kv.config.Shards[shard]{
			err = ErrWrongGroup
		}else{
			uuid, exists := kv.uuid[op.ID]
			if exists && op.UUID <= uuid{
				err = OK
			}
		}
	case "Get":
		shard := key2shard(op.Key)
		if kv.gid != kv.config.Shards[shard]{
			err = ErrWrongGroup
		}
	}
	if err != "" {
		return err, value
	}
	kv.px.Start(kv.processed_seq+1,op)
	for {
		status, val := kv.px.Status(kv.processed_seq+1)
		if status == paxos.Pending{
			time.Sleep(10*time.Millisecond)
		}else if op.UUID != val.(Op).UUID {
			kv.ApplyLog(kv.processed_seq+1, val.(Op))
			kv.px.Start(kv.processed_seq+1,op)
		}else{
			err, value = kv.ApplyLog(kv.processed_seq+1, val.(Op))
			break
		}
	}
	kv.px.Done(kv.processed_seq)
	kv.px.Min()
	return err, value
}

func (kv *ShardKV) PullShard(args *PullShardArgs, reply *PullShardReply) error {
	if kv.config.Num < args.Config.Num {
		reply.Err = ErrNotReady
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{Operation: "PullShard", UUID: nrand()}
	kv.LogOp(op)

	reply.Err = OK
	reply.DB = map[string]string{}
	reply.UUID = map[int64]int64{}

	for k, v := range kv.db {
		if key2shard(k) == args.Idx {
			reply.DB[k] = v
		}
	}
	for k, v := range kv.uuid {
		reply.UUID[k] = v
	}
	return nil
}

func (ps *PullShardReply) Merge(other PullShardReply) {
	for k, v := range other.DB {
		ps.DB[k] = v
	}
	for k, v := range other.UUID {
		uuid, exists := ps.UUID[k]
		if !exists || uuid < v {
			ps.UUID[k] = v
		}
	}
}

func (kv *ShardKV) ReconfigIt(config shardmaster.Config) bool{
	old_config := &kv.config
	merged_reply := PullShardReply{Err: OK, DB: map[string]string{}, UUID: map[int64]int64{}}
	for i := 0; i < shardmaster.NShards; i++ {
		if config.Shards[i] == kv.gid && old_config.Shards[i] != kv.gid {
			args := &PullShardArgs{Idx: i, Config: *old_config}
			reply := &PullShardReply{}
			for _, server := range old_config.Groups[old_config.Shards[i]] {
				ok := call(server, "ShardKV.PullShard", args, reply)
				if ok && reply.Err == OK {
					break
				}
				if ok && reply.Err == ErrNotReady {
					return false
				}
			}
			merged_reply.Merge(*reply)
		}
	}
	op := Op{Operation: "Reconfig", UUID: nrand(), Config: config, ReconfigContent: merged_reply}
	kv.LogOp(op)
	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	new_config := kv.sm.Query(-1)
	for num := kv.config.Num + 1; num <= new_config.Num; num++ {
		config := kv.sm.Query(num)
		if !kv.ReconfigIt(config) {
			return
		}
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.config = shardmaster.Config{}
	kv.db = map[string]string{}
	kv.uuid = map[int64]int64{}
	kv.processed_seq = 0

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
