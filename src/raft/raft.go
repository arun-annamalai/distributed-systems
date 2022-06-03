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
	"log"
	"math/rand"
	"os"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type State int64

const (
	follower State = iota
	candidate
	leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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
	currentTerm int
	// candidateId that received vote in current term (or -1 if none)
	votedFor   int
	logEntries []LogEntry
	isleader   bool
	state      State

	heartBeat         int32
	commitIndex       int
	lastApplied       int
	votes             int
	majorityVoteCount int
	quitElection      chan bool

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.mu.Unlock()
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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

	// Update term so candidate can update itself
	reply.Term = rf.currentTerm

	// update term and revert to follower
	if rf.currentTerm < args.Term {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// stale request
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	//log.Printf("Server %d | rf.votedFor %d", rf.me, rf.votedFor)
	//log.Printf("Server %d | !rf.moreUptoDate(args) %t", rf.me, !rf.moreUptoDate(args))

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && !rf.moreUptoDate(args) {
		log.Printf("Server %d votes for server %d", rf.me, args.CandidateID)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	} else {
		reply.VoteGranted = false
	}

}

// Hold lock before calling this function <= IMPORTANT
func (rf *Raft) moreUptoDate(args *RequestVoteArgs) bool {
	//log.Printf("%d\n", rf.currentTerm)
	//log.Printf("%d\n", args.LastLogTerm)

	// Both logs are empty
	if len(rf.logEntries) == 0 && args.LastLogTerm == -1 {
		return false
	} else if len(rf.logEntries) == 0 { // Voter is empty
		return false
	} else if args.LastLogTerm == -1 { // Candidate is empty
		return true
	} else { // both logs are full
		if rf.currentTerm > args.LastLogTerm {
			return true
		} else if rf.currentTerm < args.LastLogTerm {
			return false
		} else {
			return len(rf.logEntries) > args.LastLogIndex
		}
	}

}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Update term so leader can update itself
	reply.Term = rf.currentTerm

	// stale request
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// update term and revert to follower
	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	switch rf.state {
	case follower:
		// heartbeat case
		if len(args.Entries) == 0 {
			atomic.StoreInt32(&rf.heartBeat, 1)
			reply.Success = true
			return
		}

		if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
		}
		reply.Success = true
	case candidate:
		// heartbeat case
		if len(args.Entries) == 0 && rf.currentTerm == args.Term {
			atomic.StoreInt32(&rf.heartBeat, 1)
			rf.state = follower
		}
	case leader:
		log.Println("leader")
	}
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

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) electionTimeout() {
	rf.clearVote()
	atomic.StoreInt32(&rf.heartBeat, 0)
	// 200-350
	timeout := rand.Intn(150) + 200
	time.Sleep(time.Duration(timeout) * time.Millisecond)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.state = candidate
	rf.votes = 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.requestVote(i)
		}
	}
}

func (rf *Raft) requestVote(peer int) {
	args := RequestVoteArgs{}
	rf.mu.Lock()
	log.Printf("Server %d requests vote from server %d", rf.me, peer)
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	rf.mu.Unlock()

	if (len(rf.logEntries)) == 0 {
		args.LastLogTerm = -1
		args.LastLogIndex = -1
	} else {
		// TODO fill out args below
	}

	reply := RequestVoteReply{}
	rf.sendRequestVote(peer, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// reply check
	if rf.currentTerm < reply.Term {
		rf.state = follower
		rf.currentTerm = args.Term
	}

	if rf.validReply(&args, &reply) {
		log.Printf("Server %d gets vote from server %d", rf.me, peer)
		rf.votes += 1
		if rf.votes >= rf.majorityVoteCount {
			log.Printf("Server %d has majority", rf.me)
			rf.state = leader
			rf.votes = 0
		}
	}
}

func (rf *Raft) validReply(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return reply.VoteGranted && (rf.currentTerm == args.Term)
}

func (rf *Raft) clearVote() {
	rf.mu.Lock()
	rf.votes = 0
	rf.mu.Unlock()
}

func (rf *Raft) shouldStartElection() bool {
	z := atomic.LoadInt32(&rf.heartBeat)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return z == 0 && rf.state != leader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// timeout when server starts up
	rf.electionTimeout()

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.shouldStartElection() {
			go rf.startElection()
			// TODO: Handle case if election takes too long (think its already handled by using goroutine)
		}
		rf.electionTimeout()
	}
}

func (rf *Raft) createHeartBeatArgs() *AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.PrevLogTerm = -1
	args.PrevLogIndex = -1
	args.LeaderCommit = rf.commitIndex
	return &args
}

func (rf *Raft) heartBeatTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == leader {
			log.Printf("Server %d sends heartbeats as leader", rf.me)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					args := rf.createHeartBeatArgs()
					reply := AppendEntriesReply{}
					rf.mu.Unlock()
					rf.sendAppendEntries(i, args, &reply)
					rf.mu.Lock()
					// reply check
					if rf.currentTerm < reply.Term {
						rf.state = follower
						rf.currentTerm = args.Term
					}
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(110 * time.Millisecond)
	}
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
	f, err := os.OpenFile("raft-client.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	log.SetOutput(f)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	// TODO: reset the below value at appropriate times
	rf.votedFor = -1
	rf.state = follower
	rf.majorityVoteCount = (len(rf.peers) + 1) / 2
	rf.quitElection = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	log.Printf("Server %d starting", rf.me)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeatTicker()
	return rf
}
