package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	rpt      map[string]time.Time  //recent ping time from servers
	curview  View  //current view
	newview  View  //new view, become curview when ACKed 
	ACKed bool  //newview is acked or not
	backupdead  bool  //backup is dead
	primarydead  bool  //primary is dead
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	log.Printf("ping from %s, Viewnum=%d\n",args.Me,args.Viewnum)
	vs.mu.Lock()
	if vs.curview.Viewnum == 0{  //no primary server has registered
		vs.newview.Viewnum = 1
		vs.newview.Primary = args.Me
		vs.newview.Backup = ""
		vs.ACKed = true
	}else{  //there must be a primary in current view
		if args.Me == vs.curview.Primary{  //this ping comes from primary
			if args.Viewnum == 0 {  //primary crash and reboot
				vs.newview.Viewnum = vs.curview.Viewnum + 1
				if vs.curview.Backup == ""{
					vs.newview.Primary = args.Me
					vs.newview.Backup = ""
				}else{
					vs.newview.Primary = vs.curview.Backup
					vs.newview.Backup = args.Me
				}
				vs.primarydead = false
			}else if vs.backupdead == true{
				vs.newview.Viewnum = vs.curview.Viewnum + 1
				vs.newview.Primary = vs.curview.Primary
				vs.newview.Backup = ""
				vs.backupdead = false
			}
		}else if args.Me == vs.curview.Backup{  //ping from backup
			if args.Viewnum==0 {  //backup crash and reboot
				vs.backupdead = false
				vs.newview.Viewnum = vs.curview.Viewnum + 1
				vs.newview.Primary = vs.curview.Primary
				vs.newview.Backup = args.Me
			}else if vs.primarydead == true{
				vs.newview.Viewnum = vs.curview.Viewnum + 1
				vs.newview.Primary = vs.curview.Backup
				vs.newview.Backup = ""
				vs.primarydead = false
			}
		}else{  //ping comes from neithor primary nor backup
			if args.Viewnum == 0 && (vs.curview.Backup == "" || vs.backupdead==true) { //args.Me will become backup
				vs.newview.Viewnum = vs.curview.Viewnum + 1
				vs.newview.Backup = args.Me
				vs.newview.Primary = vs.curview.Primary
			}else if args.Viewnum == 0 && vs.primarydead == true{
				if vs.curview.Backup != ""{
					vs.newview.Viewnum = vs.curview.Viewnum + 1
					vs.newview.Backup = args.Me
					vs.newview.Primary = vs.curview.Backup
				}else{
					vs.newview.Viewnum = vs.curview.Viewnum + 1
					vs.newview.Backup = ""
					vs.newview.Primary = args.Me
				}
				vs.primarydead = false
			}
		}
	}
	if (args.Me == vs.curview.Primary || vs.ACKed == true) && vs.newview.Viewnum > vs.curview.Viewnum{
		vs.curview = vs.newview
		vs.ACKed = false
	}else if args.Me == vs.curview.Primary && args.Viewnum == vs.curview.Viewnum{
		vs.ACKed = true
	}
	reply.View = vs.curview
	vs.rpt[args.Me]=time.Now()
	vs.mu.Unlock()
	log.Printf("curview: Viewnum=%d, p=%s, b=%s, acked=%b\n",vs.curview.Viewnum,vs.curview.Primary,vs.curview.Backup,vs.ACKed)
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.curview

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	PrimaryInterval := time.Now().Sub(vs.rpt[vs.curview.Primary])
	BackupInterval := time.Now().Sub(vs.rpt[vs.curview.Backup])
	if vs.curview.Primary != "" && PrimaryInterval > DeadPings * PingInterval{  //Primary is dead
		log.Printf("****primary failed*****\n")
		vs.mu.Lock()
		vs.primarydead = true
		vs.mu.Unlock()
	}
	if vs.curview.Backup != "" && BackupInterval > DeadPings * PingInterval{  //Backup is dead
		log.Printf("****backup failed*****\n")
		vs.mu.Lock()
		vs.backupdead = true
		vs.mu.Unlock()
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.rpt = make(map[string]time.Time)
	vs.curview = View{}
	vs.newview = View{}
	vs.ACKed = false
	vs.backupdead = false
	vs.primarydead = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
