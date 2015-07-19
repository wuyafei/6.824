package shardkv
import "shardmaster"
//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UUID    int64    //identify of this request
	ID      int64      //identify the client

}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID    int64    //identify of this request
	ID      int64    //identify of client
}

type GetReply struct {
	Err   Err
	Value string
}

type PullShardArgs struct {
	Idx    int
	Config  shardmaster.Config
}

type PullShardReply struct {
	Err   Err
	DB    map[string]string
	UUID    map[int64]int64
}