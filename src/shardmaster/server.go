package shardmaster

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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	processed_seq   int   //paxos peers processed seq number
	current_config_num  int   //current config number
}


type Op struct {
	// Your data here.
	Action     string   //Join, Leave, Move, Query
	GID        int64    //group id, For Join and Leave
	Servers    []string    //Action Join's arguments
	Shard      int      //shard index, for Move
	Num        int      //for query
	UUID       int64
}

func (sm *ShardMaster) ExecuteAction(op Op) {
	op.UUID = nrand()
	sm.px.Start(sm.processed_seq+1, op)
	for{
		status, val := sm.px.Status(sm.processed_seq + 1)
		if status == paxos.Pending {
			time.Sleep(100 * time.Millisecond)
		}else if val.(Op).UUID != op.UUID{
			sm.ApplyAction(sm.processed_seq + 1, val.(Op))
			sm.px.Start(sm.processed_seq + 1, op)
		}else{
			sm.ApplyAction(sm.processed_seq + 1, val.(Op))
			break
		}
	}
	sm.px.Done(sm.processed_seq)
	sm.px.Min()
}

func (sm *ShardMaster) NewConfig() *Config{
	current_config := &sm.configs[sm.current_config_num]
	new_config := Config{}
	new_config.Num = current_config.Num + 1
	new_config.Groups = map[int64][]string{}
	new_config.Shards = [NShards]int64{}
	for gid, servers := range current_config.Groups {
		new_config.Groups[gid] = servers
	}
	for i, v := range current_config.Shards{
		new_config.Shards[i] = v
	}
	sm.current_config_num++
	sm.configs = append(sm.configs, new_config)
	return &sm.configs[sm.current_config_num]
}

func (sm *ShardMaster) CountShard(shards [NShards]int64) map[int64][]int {
	group = map[int64][]int{}
	for i:=0; i<NShards; i++ {
		_, exists := group[shards[i]]
		if exists {
			append(group[shards[i]], i)
		}else{
			arr := make([]int, 1)
			arr[0] = i
			group[shards[i]] = arr
		}
	}
	return group
}

func (sm *ShardMaster) ApplyAction(seq int, op Op) {
	action := op.Action
	gid = op.GID
	if action == "Join" {
		servers = op.Servers
		new_config = sm.NewConfig()
		_, exists := new_config.Groups[gid]
		if !exists {
			new_config.Groups[gid] = servers
			shards_per_group := NShards/len(new_config.Groups)
			remains_1 := NShards % len(new_config.Groups)
			remains_0 := len(new_config.Groups) - remains_1
			group_of_shard := sm.CountShard(new_config.Shards)
			if len(new_config.Groups) == 1 {
				for i:=0;i<NShards;i++ {
					new_config.Shards[i] = gid
				}
			}else{
				for _, v := range(group_of_shard) {
					over_num := 0
					if remains_1 == 0 || len(v) == shards_per_group {
						over_num = len(v) - shards_per_group
						remains_0--
					}else{
						over_num = lne(v) - shards_per_group - 1
						remains_1--
					}
					for j:=0;j<over_num;j++ {
						new_config.Shards[v[shards_per_group + j]] = gid
					}
				}
			}
		}
	}else if action == "Leave" {
		new_config = sm.NewConfig()
		_, exists := new_config.Groups[gid]
		if exists {
			delete(new_config.Groups, gid)
			shards_per_group := NShards/len(new_config.Groups)
			remains_1 := NShards % len(new_config.Groups)
			remains_0 := len(new_config.Groups) - remains_1
			group_of_shard := sm.CountShard(new_config.Shards)
			t := len(group_of_shard[gid])
			for i, v := range(group_of_shard) {
				if i == gid {
					continue
				}
				less_num := 0
				if remains_1 == 0 {
					less_num = shards_per_group - len(v)
					remains_0--
				}else{
					less_num = shards_per_group + 1 - len(v)
					remains_1--
				}
				for j:=0;j<less_num;j++ {
					new_config.Shards[group_of_shard[gid][t-j-1]] = i
				}
				t = t - less_num
			}
		}

	}else if action == "Move" {
		shard = op.shard
		new_config = sm.NewConfig()
		_, exists := new_config.Groups[gid]
		if exists && shard < NShards {
			new_config.Shards[shard] = gid
		}
	}
	sm.processed_seq = seq
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Action:"Join", GID:args.GID, Servers:args.Servers}
	sm.ExecuteAction(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Action:"Leave", GID:args.GID}
	sm.ExecuteAction(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Action:"Move", GID:args.GID, Shard: args.Shard}
	sm.ExecuteAction(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Action:"Query", Num: args.Num, UUID:nrand()}
	reply := QueryReply{}
	sm.px.Start(sm.processed_seq+1, op)
	for{
		status, val := sm.px.Status(sm.processed_seq + 1)
		if status == paxos.Pending {
			time.Sleep(100 * time.Millisecond)
		}else if val.(Op).UUID != op.UUID {
			sm.ApplyAction(sm.processed_seq + 1, val.(Op))
			sm.px.Start(sm.processed_seq + 1, op)
		}else{
			if op.Num == -1 || op.Num >= sm.current_config_num {
				reply.Config = sm.configs[sm.current_config_num]
			}else{
				reply.Config = sm.configs[op.Num]
			}
			break
		}
	}
	sm.px.Done(sm.processed_seq)
	sm.px.Min()
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.processed_seq = 0
	sm.current_config_num = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
