package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
	"fmt"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cid   int64
	seq   int64
	mu    sync.Mutex
	cacheLeader  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.cid = nrand()
	ck.seq = 0
	ck.cacheLeader = -1

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs

	ck.mu.Lock()
	ck.seq ++
	args.Seq = ck.seq
	ck.mu.Unlock()

	args.Key = key
	args.Cid = ck.cid

	// Retry until get the correct response
	for {
		if ck.cacheLeader != -1 {
			var reply GetReply
			ok := ck.servers[ck.cacheLeader].Call("RaftKV.Get", &args, &reply)

			if ok {
				if !reply.WrongLeader {
					fmt.Printf("get API response %s\n", reply)
					return reply.Value
				}
			}
		}

		for idx, server := range ck.servers {
			var reply GetReply
			ok := server.Call("RaftKV.Get", &args, &reply)

			if ok {
				if !reply.WrongLeader {
					fmt.Printf("get API response %s\n", reply)
					ck.cacheLeader = idx
					return reply.Value
				}
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// You will have to modify this function.
	var args PutAppendArgs

	ck.mu.Lock()
	ck.seq ++
	args.Seq = ck.seq
	ck.mu.Unlock()

	args.Key = key
	args.Value = value
	args.Op = op
	args.Cid = ck.cid

	// Retry until get the correct response
	for {
		for _, server := range ck.servers {
			var reply PutAppendReply

			ok := server.Call("RaftKV.PutAppend", &args, &reply)

			if ok {
				if !reply.WrongLeader {
					fmt.Printf("put API response %s\n", reply)
					return
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
