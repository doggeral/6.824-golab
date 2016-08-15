package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"fmt"
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

		channel,ok := kv.res[index]

		if !ok {
			channel := make(chan Op)
			kv.res[index] = channel
		}

		select {
		case msg:= <- channel:
			if msg == command {
				reply.Err = OK
				reply.Value = kv.kvMap[args.Key]
			} else {
				reply.Err = Error
			}
		case <-time.After(time.Duration(500) * time.Millisecond):
			reply.Err = Timeout
		}
	}

	fmt.Printf("Reply Get : %s \n", reply)

	return

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var command Op
	command.Type = "Put"
	command.Key = args.Key
	command.Value = args.Value
	command.Cid = args.Cid
	command.Seq = args.Seq

	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false

		channel,ok := kv.res[index]
		if !ok {
			channel := make(chan Op)
			kv.res[index] = channel
		}

		select {
		case msg:= <- channel:
			if msg == command {
				reply.Err = OK
			} else {
				reply.Err = Error
			}
		case <-time.After(time.Duration(500) * time.Millisecond):
			reply.Err = Timeout
		}
	}

	fmt.Printf("Reply PutAppend : %s \n", reply)

	return

}

func (kv *RaftKV) Execute(op *Op) {
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
					channel := make(chan Op)
					kv.res[index] = channel
				} else {
					fmt.Printf("Return commit message: %s \n", msg)
					channel <- op
				}
				kv.mu.Unlock()

			}
		}
	}()

	return kv
}
