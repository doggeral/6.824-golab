package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	"time"
)

const Debug = 1

const (
	SUCCESS = iota
	FAIL
)

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
	Type   string
	Key    string
	Value  string
	Cid    int64
	Seq    int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// map which stores the KV value
	kvMap   map[string]string
	reqMap  map[int64]int

	res     map[int]chan Op
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var command Op
	command.Type = "Get"
	command.Key = args.Key
	command.Cid = args.Cid
	command.Seq = args.Seq

	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		fmt.Printf("Get ... index: %d\n", index)

		_,ok := kv.res[index]

		if !ok {
			kv.res[index] = make(chan Op, 1)
		}

		select {
		// !!!Notice that it should use kv.res[index] instead to assigning kv.res[index] to a variable because
		// the assignment operation will copy the content hence the listening on the selector never returns.
		case msg:= <- kv.res[index]:
			reply.Err = OK
			reply.Value = kv.kvMap[args.Key]
			fmt.Printf("Reply Get : %s, msg: %s \n", reply, msg)
			if msg == command {
				reply.Err = OK
				reply.Value = kv.kvMap[args.Key]
				fmt.Printf("Reply Get : %s \n", reply)
			} else {
				reply.Err = Error
				reply.WrongLeader = true
			}
		case <-time.After(time.Duration(1000) * time.Millisecond):
			reply.Err = Timeout
			reply.WrongLeader = true
			fmt.Printf("Reply Get timeout\n")
		}
	}

	return

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var command Op
	command.Type = args.Op
	command.Key = args.Key
	command.Value = args.Value
	command.Cid = args.Cid
	command.Seq = args.Seq

	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.WrongLeader = true
	} else {
		fmt.Printf("PutAppend index: %d\n", index)
		reply.WrongLeader = false
		//time.Sleep(time.Duration(2000) * time.Millisecond)
		_,ok := kv.res[index]
		if !ok {
			kv.res[index] =  make(chan Op, 1)
		}

		select {
		case msg:= <- kv.res[index]:
			reply.Err = OK
			fmt.Printf("Reply PutAppend : %s, msg: %s \n", reply, msg)
			//if msg == command {
			//	reply.Err = OK
			//	fmt.Printf("Reply PutAppend : %s \n", reply)
			//} else {
			//	reply.Err = Error
			//	reply.WrongLeader = true
			//	fmt.Printf("Reply PutAppend : %s \n", reply)
			//}
		case <-time.After(time.Duration(1000) * time.Millisecond):
			reply.Err = Timeout
			reply.WrongLeader = true
			fmt.Printf("Reply append timeout\n")
		}
	}

	return

}

func (kv *RaftKV) Execute(op *Op) {
	fmt.Printf("Op is %s\n", op)
	switch op.Type {
	case "Put":
		kv.kvMap[op.Key] = op.Value
	case "Append":
		kv.kvMap[op.Key] += op.Value
	}
}
//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.res = make(map[int] chan Op)
	kv.kvMap = make(map[string]string)
	kv.reqMap = make(map[int64]int)

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func (){
		for {
			// listen the commit message
			select {
			case msg:= <- kv.applyCh:
				fmt.Printf("Get commit message: %s \n", msg)

				op := msg.Command.(Op)
				kv.mu.Lock()

				kv.Execute(&op)

				// ack client that the operation has been commited
				index := msg.Index

				channel,ok := kv.res[index]

				if !ok {
					channel = make(chan Op, 1)
					kv.res[index] = channel
				} else {
					fmt.Printf("Return commit message: %s; op: %s, \n", msg, op)
					kv.res[index] <- op
				}
				kv.mu.Unlock()

			}
		}
	}()

	return kv
}
