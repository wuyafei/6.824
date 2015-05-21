package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view       viewservice.View   //current view
	role       string   //Primary, Backup, Neither
	db         map[string]string  //database to store key/value pair
	uuid       map[int64]int64  //client_id => latest uuid, for at-most-once strategy
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	//Get() don't need at-most-once strategy	
	if pb.role != "Primary"{  // if not primary, return error
		reply.Err = ErrWrongServer
	}else{
		pb.mu.Lock()
		Val, KeyExists := pb.db[args.Key]
		if KeyExists{
			reply.Value = Val
			reply.Err = OK
		}else{  // if no key exists, err
			reply.Err = ErrNoKey
			reply.Value = ""
		}
		pb.mu.Unlock()
	}
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	if args.UUID == pb.uuid[args.ID]{  // ensure at-most-once Put or Append
		reply.Err = OK
		pb.mu.Unlock()
		return nil
	}
	if pb.role == "Neither"{
		reply.Err = ErrWrongServer
	}else if pb.role == "Primary"{
		if args.IsForward {   //the msg is forwarded, no longer primary any more
			reply.Err = ErrWrongServer
		}else{
			if args.Op == "Put"{
				pb.db[args.Key] = args.Value
			}else{
				pb.db[args.Key] = pb.db[args.Key] + args.Value
			}
			pb.uuid[args.ID]=args.UUID
			//forward to backup
			if pb.view.Backup != ""{
				forwardArgs := args
				forwardArgs.IsForward = true  //mark this msg as forward
				forwardReply := &PutAppendReply{}
				for call(pb.view.Backup, "PBServer.PutAppend", forwardArgs, forwardReply)==false || forwardReply.Err == ErrWrongServer{
					time.Sleep(viewservice.PingInterval)
					v,_ := pb.vs.Ping(pb.view.Viewnum)
					pb.view = v
					if v.Backup == ""{  //Backup is dead
						break
					}
				}
			}
			reply.Err = OK
		}
	}else {  //role is Backup
		if args.IsForward == false{  //direct msg from client, decline it
			reply.Err = ErrWrongServer
		}else{
			if args.Op == "Put"{
				pb.db[args.Key] = args.Value
			}else{  //append
				pb.db[args.Key] = pb.db[args.Key] + args.Value
			}
			pb.uuid[args.ID]=args.UUID
			reply.Err = OK
		}
	}
	pb.mu.Unlock()
	return nil
}


//Transfer the complete db and uuid to backup
func (pb *PBServer) Transfer(args *TranArgs, reply *TranReply) error{
	pb.mu.Lock()
	pb.db = args.DB
	pb.uuid = args.UUID
	pb.mu.Unlock()
	reply.Err = OK
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	v := viewservice.View{}
	if pb.role == "Neither"{  //not primary nor backup,
		v, _ = pb.vs.Ping(0)  //so ping(0) to be a volunteer of backup
	}else{
		v, _ = pb.vs.Ping(pb.view.Viewnum)  //get the current view from view server
	}
	if pb.me == v.Primary{
		pb.role = "Primary"
	}else if pb.me == v.Backup{
		pb.role = "Backup"
	}else{
		pb.role = "Neither"
	}
	if pb.role == "Primary" && v.Backup!= "" && v.Backup != pb.view.Backup{ //backup has changed, need a transfer
		args := &TranArgs{pb.db, pb.uuid}
		reply := &TranReply{}
		for call(v.Backup, "PBServer.Transfer", args, reply) == false{
			time.Sleep(viewservice.PingInterval)
		}
	}
	pb.view = v
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.db = make(map[string]string)
	pb.view = viewservice.View{}
	pb.role = "Neither"
	pb.uuid = make(map[int64]int64)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
