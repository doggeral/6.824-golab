package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	Duplicate      = "Duplicate request"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string
	IsReplica bool
	ReqId     string
	ClientId  string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}


type SyncArgs struct {
	Type  string
	ReqId     string
	ClientId  string
	KVMap   map[string]string
	NeedClear bool
}

type SyncReply struct {
	Err Err
}
// Your RPC definitions here.
