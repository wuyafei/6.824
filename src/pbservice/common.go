package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op    string    //"Put" or "Append"
	IsForward  bool   //the request is forwared by primary or directly from client
	UUID  int64    //id of the request itself
	ID    int64    //id of the client who sends this request

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UUID  int64   //id of the request itself
	ID    int64   //id of the client who sends this request
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
type TranArgs struct{
	DB  map[string]string
	UUID map[int64]int64
}

type TranReply struct{
	Err Err
}
