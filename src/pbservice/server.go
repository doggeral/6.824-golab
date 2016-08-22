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

	kvMap      map[string]string
	lovalView  viewservice.View
	state      string
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	if pb.isActivePrimary() {
		reply.Value = pb.kvMap[args.Key]
	} else {
		reply.Err = "Not primary"
	}

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.isActivePrimary() && pb.lovalView.Backup != "" {
		replicaArgs := PutAppendArgs{Key:args.Key, Type:args.Type, Value:args.Value, IsReplica:true}
		success := pb.doReplication(pb.lovalView.Backup, &replicaArgs)

		if !success {
			reply.Err = "Replicate to backup error!"
			return nil
		}
	}

	if pb.isActivePrimary() || args.IsReplica {
		if args.Type == "Put" {
			pb.kvMap[args.Key] = args.Value
		} else if args.Type == "Append" {
			val,ok := pb.kvMap[args.Key]

			if ok {
				pb.kvMap[args.Key] = val + args.Value
			} else {
				pb.kvMap[args.Key] = args.Value
			}
		}
	} else {
		reply.Err = "The server is not primary or not valid to do the replication"
	}


	return nil
}

func (pb *PBServer) isActivePrimary () bool{
	if pb.state == "PRIMARY" {
		return true
	}

	return  false
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	view,_ := pb.vs.Ping(pb.lovalView.Viewnum)

	// Replica all data to new backup server
	if pb.isActivePrimary() && view.Backup != "" && pb.lovalView.Backup != view.Backup {
		log.Println("%s replica all", pb.me)
		for key,value := range pb.kvMap {
			replicaArgs := PutAppendArgs{Key:key, Type:"Put", Value:value, IsReplica:true}
			pb.doReplication(view.Backup, &replicaArgs)
		}
	}
	pb.lovalView = view

	if view.Primary == pb.me {
		pb.state = "PRIMARY"
	} else if view.Backup == pb.me {
		pb.state = "BACKUP"
	} else {
		pb.state = "NONE"
	}

}

func (pb *PBServer) doReplication(backup string, args *PutAppendArgs) bool {
	// replica the data to backup
	backupReply := PutAppendReply{}
	ok := call(backup, "PBServer.PutAppend", &args, &backupReply)
	if ok == false {
		log.Println("Replica error, %s", args)
		return false
	}

	return true
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
	pb.kvMap = make(map[string] string)
	pb.state = "NONE"
	pb.lovalView = viewservice.View{0, "", ""}

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
