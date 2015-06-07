package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

const (
	OK = 1
	Reject = 2
)

type Agreement struct {
	State    Fate
	N_p      int64
	N_a      int64
	V_a      interface{}
}

type PrepArgs struct{
	Seq    int    //seq number of agreement instance
	N        int64    //prepare number
}

type PrepReply struct{
	Resp     int      //ok, reject 
	N_a      int64    //acceptor's n_a
	V_a      interface{}    //acceptor's v_a
	MaxForgotten    int
}

type AcptArgs struct{
	Seq    int
	N        int64
	V        interface{}     //accept value
}

type AcptReply struct{
	Resp     int      //ok, reject 
	MaxForgotten    int
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	Agrees    map[int]*Agreement     //map from seq => Agreement
	pn        int      //number of peers, used for generate different N
	Dones      []int   //record for Done

}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


func (px *Paxos) proposer(seq int, v interface{}) {
	for{
		agre, exists := px.Agrees[seq]
		if exists && (agre.State==Decided || agre.State==Forgotten){
			break
		}
		n := time.Now().Unix()*int64(px.pn) + int64(px.me);
		cnt := 0
		var max_n_a int64 = 0
		ret_v := v
		for i:=0;i<px.pn;i++{       //starting prepare here
			args := &PrepArgs{seq, n}
			reply := &PrepReply{}
			if i == px.me {
				px.Prepare(args, reply)
			}else{
				call(px.peers[i], "Paxos.Prepare", args, reply)
			}
			if reply.MaxForgotten > px.Dones[i]{      //update Dones
				px.Dones[i]=reply.MaxForgotten
			}
			if reply.Resp == OK{
				cnt++
				if reply.N_a > max_n_a{
					max_n_a = reply.N_a
					ret_v = reply.V_a
				}
				if cnt > px.pn/2 {     //majority prepare_ok
					break
				}
			}
		}
		if cnt>px.pn/2{     //prepare ok, start sending accept
			cnt = 0
			for i:=0;i<px.pn;i++{
				args := &AcptArgs{seq, n, ret_v}
				reply := &AcptReply{}
				if i == px.me {
					px.Accept(args, reply)
				}else{
					call(px.peers[i], "Paxos.Accept", args, reply)
				}
				if reply.MaxForgotten > px.Dones[i]{
					px.Dones[i]=reply.MaxForgotten
				}
				if reply.Resp == OK{
					cnt++
					if cnt > px.pn/2 {    //majority accept_ok
						break
					}
				}
			}
		}
		if cnt>px.pn/2{     //accept ok, start sending decide
			for i:=0;i<px.pn;i++{
				args := &AcptArgs{seq, n, ret_v}
				reply := &AcptReply{}
				if i==px.me{
					px.Decide(args, reply)
				}else{
					call(px.peers[i], "Paxos.Decide", args, reply)
				}
				//here we don't care if we get majority decide_ok

				if reply.MaxForgotten > px.Dones[i]{
					px.Dones[i]=reply.MaxForgotten
				}
			}
			break
		}
		//if prepare or accpet did not get majority ok, we wait for a random time to re_prepare/accept
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		tt := time.Duration(r.Intn(20)) * time.Millisecond
		time.Sleep(tt)
	}
}

//handler of acceptor's prepare
func (px *Paxos) Prepare(args *PrepArgs, reply *PrepReply) error{
	px.mu.Lock()
	_, exists := px.Agrees[args.Seq]
	if !exists{
		px.Agrees[args.Seq] = &Agreement{State:Pending, N_p:args.N}
		reply.Resp = OK
		reply.N_a = 0
		reply.V_a = px.Agrees[args.Seq].V_a
	}else if args.N > px.Agrees[args.Seq].N_p {
		px.Agrees[args.Seq].N_p = args.N
		reply.Resp = OK
		reply.N_a = px.Agrees[args.Seq].N_a
		reply.V_a = px.Agrees[args.Seq].V_a
	}else{
		reply.Resp = Reject
	}
	reply.MaxForgotten= px.Dones[px.me]
	px.mu.Unlock()
	return nil
}

//handler of acceptor's accept
func (px *Paxos) Accept(args *AcptArgs, reply *AcptReply) error{
	px.mu.Lock()
	_, exists := px.Agrees[args.Seq]
	if !exists{
		px.Agrees[args.Seq] = &Agreement{State:Pending, N_p:args.N, N_a:args.N, V_a:args.V}
		reply.Resp = OK
	}else if args.N >= px.Agrees[args.Seq].N_p {
		px.Agrees[args.Seq].N_p = args.N
		px.Agrees[args.Seq].N_a = args.N
		px.Agrees[args.Seq].V_a = args.V
		reply.Resp = OK
	}else{
		reply.Resp = Reject
	}
	reply.MaxForgotten= px.Dones[px.me]
	px.mu.Unlock()
	return nil
}

//handler of decide
func (px *Paxos) Decide(args *AcptArgs, reply *AcptReply) error{
	px.mu.Lock()
	_, exists := px.Agrees[args.Seq]
	if !exists{
		px.Agrees[args.Seq] = &Agreement{State:Decided, N_p:args.N, N_a:args.N, V_a:args.V}
	}else {
		px.Agrees[args.Seq].State = Decided
		px.Agrees[args.Seq].N_p = args.N
		px.Agrees[args.Seq].N_a = args.N
		px.Agrees[args.Seq].V_a = args.V
	}
	//fmt.Printf("i:%v, decided! seq: %v\n",px.me, args.Seq)
	reply.Resp = OK
	reply.MaxForgotten= px.Dones[px.me]
	px.mu.Unlock()
	return nil
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	if seq < px.Min(){
		return
	}
	go func(){
		px.proposer(seq, v)
	}()
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	if seq < px.Dones[px.me]{
		return
	}
	px.mu.Lock()
	for key,_ := range px.Agrees{
		if key<=seq{
			px.Agrees[key].State = Forgotten
		}
	}
	px.Dones[px.me]=seq
	px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	max := px.Dones[px.me]
	for key,_ := range px.Agrees{
		if key> max{
			max = key
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	min_fgot := px.Dones[px.me]
	for i:=0;i<px.pn;i++{
		if min_fgot > px.Dones[i]{
			min_fgot = px.Dones[i]
		}
	}
	px.mu.Lock()
	for k,_ := range px.Agrees{
		if k<= min_fgot {
			delete(px.Agrees, k)
		}
	}
	px.mu.Unlock()
	return min_fgot+1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	if seq < px.Min(){
		return Forgotten, nil
	}
	val, exists := px.Agrees[seq]
	if !exists {
		return Pending, nil
	}
	return val.State, val.V_a
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
	px.Agrees = make(map[int]*Agreement)
	px.pn = len(peers)
	px.Dones = make([]int, px.pn)
	for i:=0;i<px.pn;i++{
		px.Dones[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
