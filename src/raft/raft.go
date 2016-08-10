package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"fmt"
	"labrpc"
	"math/rand"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role int

	// channel msg
	resetTimeoutMsg chan bool
	voteGrantedMsg  chan bool
	roleChangeMsg   chan bool
	commitMsg       chan bool

	voteNum int

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Command interface{}
	LogTerm int
}

func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.role == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here.
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	LastIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term

		if rf.role != FOLLOWER {
			rf.role = FOLLOWER
			rf.roleChangeMsg <- true
		}
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	// Check candidate's log
	var isUptoDate bool

	if args.LastLogTerm == rf.getLastTerm() {
		isUptoDate = args.LastLogIndex >= rf.getLastIndex()
	} else {
		isUptoDate = args.LastLogTerm < rf.getLastTerm()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId && isUptoDate {
		if rf.role != FOLLOWER {
			rf.role = FOLLOWER
			rf.roleChangeMsg <- true
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.voteGrantedMsg <- true
	}

	fmt.Printf("currentTerm: %d, vote for %d, term %d \n", rf.currentTerm, args.CandidateId, args.Term)

	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// all the servers need to do
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			if rf.role != FOLLOWER {
				rf.role = FOLLOWER
				rf.roleChangeMsg <- true
			}
			rf.votedFor = -1
		}

		if rf.role == CANDIDATE {
			if reply.VoteGranted {
				rf.voteNum++

				if rf.voteNum > len(rf.peers)/2 {
					rf.role = LEADER
					// init the nextIndex
					rf.nextIndex = make([]int, len(rf.peers))
					index := rf.getLastIndex() + 1
					for idx := 0; idx < len(rf.peers); idx++ {
						rf.nextIndex[idx] = index
					}
					rf.roleChangeMsg <- true
				}
			}
		}

	}
	return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.resetTimeoutMsg <- true

	// heart beat
	if len(args.Entries) == 0 {
		return
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		return
	}

	item := rf.log[args.PrevLogIndex]

	if args.PrevLogTerm != item.LogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		return
	}

	newLogEntry := rf.log[0 : args.PrevLogIndex+1]
	newLogEntry = append(newLogEntry, args.Entries...)
	rf.currentTerm = args.Term
	rf.log = newLogEntry
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.LastIndex = rf.getLastIndex()

	if args.LeaderCommit > rf.commitIndex {
		lastIdx := rf.getLastIndex()
		if lastIdx > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIdx
		}

		rf.commitMsg <- true
	}

	fmt.Printf(".... server %d appends log!! log entry: %d, term: %d, lastIndex: %d\n", rf.me, rf.log, reply.Term, reply.LastIndex)

	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == LEADER {
		if ok {
			if reply.Success {
				rf.nextIndex[server] = reply.LastIndex + 1
			}
		}
	}

	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.role == LEADER

	if isLeader {
		// update the log entry
		rf.log = append(rf.log, LogEntry{command, term})
		index = rf.getLastIndex()
		rf.commitIndex = index
		rf.commitMsg <- true
		fmt.Printf(".... server %d log the entry command: %s, term: %d, index: %d\n", rf.me, command, term, index)
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.resetTimeoutMsg = make(chan bool)
	rf.roleChangeMsg = make(chan bool)
	rf.voteGrantedMsg = make(chan bool)
	rf.commitMsg = make(chan bool)

	go startDemon(rf)
	go commitHandler(rf, applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func startDemon(rf *Raft) {
	go followerDeamon(rf)
	go leaderDeamon(rf)
	go candidateDeamon(rf)
}

func followerDeamon(rf *Raft) {
	fmt.Printf(".... start follower deamon %d \n", rf.me)
	for {
		if rf.role == FOLLOWER {
			select {
			case <-rf.roleChangeMsg:
			case <-rf.resetTimeoutMsg:
			case <-rf.voteGrantedMsg:
			case <-time.After(time.Duration(rand.Int63()%300+100) * time.Millisecond):
				fmt.Printf(".... time out %d \n", rf.me)
				sendVoteToAllServers(rf)
			}
		} else {
			select {
			case <-rf.roleChangeMsg:
			}
		}
	}
}

func leaderDeamon(rf *Raft) {
	fmt.Printf(".... start leader deamon %d \n", rf.me)
	for {
		if rf.role == LEADER {
			select {
			case <-rf.roleChangeMsg:
			case <-time.After(time.Duration(50) * time.Millisecond):
				fmt.Printf(".... time out %d \n", rf.me)
				sendAppendLogToAllServers(rf)
			}
		} else {
			select {
			case <-rf.roleChangeMsg:
			}
		}
	}
}

func candidateDeamon(rf *Raft) {
	fmt.Printf(".... start candidate deamon %d \n", rf.me)
	for {
		if rf.role == CANDIDATE {
			select {
			case <-rf.roleChangeMsg:
			case <-time.After(time.Duration(350) * time.Millisecond):
				fmt.Printf(".... time out %d \n", rf.me)
			}
		} else {
			select {
			case <-rf.roleChangeMsg:
			}
		}
	}
}

func commitHandler(rf *Raft, applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.commitMsg:
			fmt.Println("### %d, %d, %d", rf.me,rf.lastApplied,rf.commitIndex)
			commitIndex := rf.commitIndex
			for i := rf.lastApplied+1; i <= commitIndex; i++ {
				msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
				applyCh <- msg
				rf.lastApplied = i
			}
		}
	}
}

func sendVoteToAllServers(rf *Raft) {
	fmt.Printf(".... send vote by %d \n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == FOLLOWER {
		// Change to candidate
		rf.role = CANDIDATE
		rf.currentTerm++
		rf.voteNum = 1
		rf.roleChangeMsg <- true

		var args RequestVoteArgs
		args.CandidateId = rf.me
		args.Term = rf.currentTerm
		args.LastLogIndex = rf.getLastIndex()
		args.LastLogTerm = rf.getLastTerm()

		for idx, _ := range rf.peers {
			if idx != rf.me {
				var reply RequestVoteReply
				go rf.sendRequestVote(idx, args, &reply)
			}
		}
	}
}

func sendAppendLogToAllServers(rf *Raft) {
	fmt.Printf(".... send appendlog by %d \n", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == LEADER {
		for idx, _ := range rf.peers {
			if idx != rf.me {
				var reply AppendEntriesReply
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[idx] - 1

				args.PrevLogTerm = rf.log[args.PrevLogIndex].LogTerm
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1:]))
				copy(args.Entries, rf.log[args.PrevLogIndex+1:])
				args.LeaderCommit = rf.commitIndex

				fmt.Printf("!!! IDX: %d, index: %d, entries: %d, rf-log: %d \n", idx, rf.nextIndex[idx] - 1, args.Entries, rf.log)
				go rf.sendAppendEntries(idx, args, &reply)
			}

		}
	}
}
