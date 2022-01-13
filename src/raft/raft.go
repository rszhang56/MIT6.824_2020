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

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "bytes"
	// "../labgob"

	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	heartsIntervel   time.Duration = 100 * time.Millisecond
	followerInterval time.Duration = 150 * time.Millisecond
)

type ServerState int

const (
	Follower  ServerState = 1
	Candidate ServerState = 2
	Leader    ServerState = 3
)

type Entry struct {
	EntryTerm    int
	EntryCommand interface{}
	EntryIndex   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       ServerState
	timeOuts    int
	lastReceive time.Time
	applyCh     chan ApplyMsg

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Entry
	num         int

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

func MyMin(x, y int) int {
	if x < y {
		return x
	}
	return y
}
func MyMax(x, y int) int {
	if x < y {
		return y
	}
	return x
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == rf.me) && rf.log[len(rf.log)-1].EntryTerm <= args.LastLogTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		if rf.state == Leader {
			rf.state = Follower
		}
	}
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevEntry    Entry
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term      int
	Success   bool
	IsChanged int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LastCommitindex := rf.commitIndex
	rf.votedFor = -1
	rf.lastReceive = time.Now()
	if rf.state != Follower && rf.log[len(rf.log)-1].EntryTerm <= args.PrevEntry.EntryTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		reply.Success = false
		return
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.log[len(rf.log)-1].EntryTerm > args.PrevEntry.EntryTerm && rf.currentTerm > args.Term {
		reply.IsChanged = 1
		return
	}
	rf.currentTerm = args.Term
	if len(args.Entries) == 0 {
		rf.commitIndex = MyMin(args.LeaderCommit, len(rf.log)-1)
	} else {
		rf.commitIndex = LastCommitindex
		if len(rf.log) > args.PrevEntry.EntryIndex {
			if rf.log[args.PrevEntry.EntryIndex].EntryTerm == args.PrevEntry.EntryTerm {
				reply.Success = true
				if len(rf.log) == args.PrevEntry.EntryIndex+1 {
					rf.log = append(rf.log, args.Entries...)
				} else {
					rf.log = rf.log[:args.PrevEntry.EntryIndex+1]
					rf.log = append(rf.log, args.Entries...)
				}
				if rf.commitIndex < args.LeaderCommit {
					rf.commitIndex = MyMin(args.Entries[len(args.Entries)-1].EntryIndex, args.LeaderCommit)
					rf.commitIndex = MyMin(rf.commitIndex, len(rf.log)-1)
				}
			} else {
				return
			}
		} else {
			return
		}
	}
	if rf.lastApplied != rf.commitIndex {
		for i := LastCommitindex + 1; i <= rf.commitIndex; i++ {
			NewApplyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].EntryCommand,
				CommandIndex: i,
			}
			rf.applyCh <- NewApplyMsg
		}
		rf.lastApplied = rf.commitIndex
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isLeader = true
		index = rf.log[len(rf.log)-1].EntryIndex + 1
		term = rf.currentTerm
		NewEntry := Entry{EntryTerm: rf.currentTerm, EntryIndex: index, EntryCommand: command}
		rf.log = append(rf.log, NewEntry)
		return index, term, isLeader
	} else {
		return -1, -1, false
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) monitor() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		switch rf.state {
		case Leader:
			rf.mu.Unlock()
			rf.doLeader()
			break
		case Follower:
			rf.mu.Unlock()
			rf.doFollower()
			break
		case Candidate:
			rf.mu.Unlock()
			rf.doElection()
			break
		default:
			panic("Invalid  state")
		}
	}
}

func (rf *Raft) checkState(curState ServerState) bool {
	if rf.killed() || rf.state != curState {
		return false
	}
	return true
}

func (rf *Raft) doLeader() {
	for {
		rf.mu.Lock()
		me := rf.me
		num := rf.num
		if !rf.checkState(Leader) {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := 0; i < num; i++ {
			if i == me {
				continue
			}
			go func(server int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevEntry:    Entry{},
					Entries:      []Entry{},
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{
					Term:      0,
					Success:   false,
					IsChanged: 0,
				}
				NumLogs := len(rf.log)
				nextIndex := rf.nextIndex[server]
				if nextIndex < NumLogs {
					for i := nextIndex; i < NumLogs; i++ {
						args.Entries = append(args.Entries, rf.log[i])
					}
					args.PrevEntry = rf.log[nextIndex-1]
					args.PrevEntry.EntryCommand = ""
					if rf.matchIndex[server] > nextIndex || !rf.checkState(Leader) {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					if !rf.sendAppendEntries(server, &args, &reply) {
						return
					}
					rf.mu.Lock()
					if !rf.checkState(Leader) {
						rf.mu.Unlock()
						return
					}
					if reply.Success == false {
						rf.nextIndex[server] = MyMax(rf.nextIndex[server]-10, 1)
					} else {
						lastMatchedIndex := rf.matchIndex[server]
						rf.matchIndex[server] = MyMin(NumLogs-1, len(rf.log)-1)
						rf.nextIndex[server] = MyMin(NumLogs, len(rf.log))
						if rf.matchIndex[server] != lastMatchedIndex && args.Entries[len(args.Entries)-1].EntryTerm == rf.currentTerm {
							rf.updateMatchIndex(server)
						}
					}
					rf.mu.Unlock()
				} else {
					rf.mu.Unlock()
					args.PrevEntry = Entry{}
					if !rf.sendAppendEntries(server, &args, &reply) {
						return
					}
				}
				rf.mu.Lock()
				if !rf.checkState(Leader) {
					rf.mu.Unlock()
					return
				}
				if reply.IsChanged == 1 {
					rf.state = Follower
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.log = rf.log[:rf.commitIndex+1]
				}
				rf.mu.Unlock()
			}(i)
		}
		time.Sleep(heartsIntervel)
	}
}

func (rf *Raft) updateMatchIndex(server int) {
	if rf.commitIndex >= rf.matchIndex[server] {
		return
	}
	agreeCnt := 1
	for j := 0; j < rf.num; j++ {
		if rf.matchIndex[j] >= rf.matchIndex[server] {
			agreeCnt++
		}
	}
	DPrintf("leader [%d] collects agreements at index %d: %v", rf.me, rf.matchIndex[server], agreeCnt)
	if agreeCnt > (rf.num / 2) {
		for i := rf.commitIndex + 1; i <= rf.matchIndex[server]; i++ {
			NewApplyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].EntryCommand,
				CommandIndex: i,
			}
			rf.applyCh <- NewApplyMsg
		}
		rf.commitIndex = rf.matchIndex[server]
		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) doFollower() {
	rf.mu.Lock()
	rf.lastReceive = time.Now()
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		rf.timeOuts = rand.Intn(200) + 600
		t := time.Now()
		waitTime := int(t.Sub(rf.lastReceive).Milliseconds())
		if waitTime > rf.timeOuts {
			rf.state = Candidate
			rf.currentTerm++
			rf.mu.Unlock()
			return
		}
		if rf.killed() || rf.state != Follower {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(followerInterval)
	}
}

func (rf *Raft) doElection() bool {
	rf.mu.Lock()
	curPeers := rf.peers
	me := rf.me
	rf.votedFor = rf.me
	rf.lastReceive = time.Now()
	rf.timeOuts = rand.Intn(200) + 600
	timeOut := rf.timeOuts
	rf.mu.Unlock()
	votedNum := 1
	count := 1
	for i := 0; i < len(curPeers); i++ {
		if i == me {
			continue
		}
		go func(server int) {
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}
			rf.mu.Lock()
			args.CandidateId = rf.me
			args.Term = rf.currentTerm
			args.LastLogIndex = rf.log[len(rf.log)-1].EntryIndex
			args.LastLogTerm = rf.log[len(rf.log)-1].EntryTerm
			rf.mu.Unlock()
			if !rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				count++
				rf.mu.Unlock()
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.state = Follower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
			} else if reply.VoteGranted {
				votedNum++
			}
			count++
			rf.mu.Unlock()
			return
		}(i)
	}
	t0 := time.Now()
	for time.Since(t0).Milliseconds() < int64(timeOut) {
		rf.mu.Lock()
		if votedNum > len(curPeers)/2 || count >= len(rf.peers) {
			rf.mu.Unlock()
			break
		}
		if rf.state != Candidate || rf.killed() {
			rf.mu.Unlock()
			return false
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = -1
	if rf.state != Candidate || votedNum <= len(curPeers)/2 {
		rf.state = Follower
		return false
	} else {
		rf.state = Leader
		nextIndex := MyMax(1, rf.log[len(rf.log)-1].EntryIndex)
		for i := 0; i < rf.num; i++ {
			rf.nextIndex[i] = nextIndex
			rf.matchIndex[i] = 0
		}
		return true
	}
	return false
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

	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	rf.num = len(rf.peers)
	rf.votedFor = 0
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.timeOuts = rand.Intn(200) + 300
	rf.state = Follower
	rf.lastReceive = time.Now()
	rf.log = append(rf.log, Entry{EntryTerm: 0, EntryCommand: "empty", EntryIndex: 0})
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.monitor()
	// start ticker goroutine to start elections
	// go rf.ticker()

	return rf
}
