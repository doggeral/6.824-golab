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
	currentView   View
	currenTick    uint
	primaryTick   uint
	primaryACK    uint
	backupTick    uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	log.Println("ping...")

	name := args.Me
	num := args.Viewnum

	switch name {
	case vs.currentView.Primary:
		if num == vs.currentView.Viewnum {
			vs.primaryACK = num
			vs.primaryTick = vs.currenTick
		} else {
			// change the backup to primary
			if vs.isACKed() {
				vs.currentView.Viewnum ++
				vs.currentView.Primary = vs.currentView.Backup
				vs.currentView.Backup = ""
				vs.primaryTick = vs.backupTick
			}
		}
	case vs.currentView.Backup:
		if (num != vs.currentView.Viewnum) {
			if vs.isACKed() {
				vs.currentView.Backup = ""
			}
		} else {
			vs.backupTick = vs.currenTick
		}
	default:
		// view server init, accept the first client which pings it as primary.
		if vs.currentView.Viewnum == 0 {
			log.Println("get the leader...")
			vs.currentView.Primary = name
			vs.currentView.Viewnum++
			vs.primaryTick = vs.currenTick
			vs.primaryACK = 0
			log.Println("current view %s", vs.currentView)
			break;
		}

		if vs.currentView.Backup == "" {
			if vs.isACKed() {
				vs.currentView.Backup = name
				vs.currentView.Viewnum++
				vs.backupTick = vs.currenTick
				log.Println("get back up, name is %s", vs.currentView.Backup)
			}
		}
	}

	reply.View = vs.currentView

	return nil
}


func (vs *ViewServer) isACKed() bool{
	return vs.currentView.Viewnum == vs.primaryACK
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.currentView
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	log.Println("tick...")
	// Your code here.
	vs.currenTick ++

	primary := vs.currentView.Primary
	backup  := vs.currentView.Backup

	if primary != "" && vs.primaryTick + DeadPings < vs.currenTick {
		// change the primary
		if vs.isACKed() {
			vs.currentView.Viewnum ++
			vs.currentView.Primary = backup
			vs.currentView.Backup = ""
			vs.primaryTick = vs.backupTick
		}
	}

	if backup != "" && vs.backupTick + DeadPings < vs.currenTick {
		if vs.isACKed() {
			vs.currentView.Viewnum ++
			vs.currentView.Backup = ""
		}
	}

	log.Println("Current view is %s", vs.currentView)

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

	vs.backupTick = 0
	vs.currenTick = 0
	vs.currentView = View{Primary:"", Backup:"", Viewnum:0}
	vs.primaryACK = 999999

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
