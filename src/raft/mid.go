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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"fmt"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	isLeader    bool
	ticks       int64
	rem         int64
	role        int32
	phase       int32
	votesRec    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (ct int, l bool) {
	// Your code here (2A).
	// p := make(atomic.Pointer[Raft]) CHECK THIS OUT LATER
	rf.mu.Lock()

	ct = rf.currentTerm
	l = rf.isLeader

	rf.mu.Unlock()

	return
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntries struct {
	Term   int
	Server int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
    fmt.Printf("I am %d receiving request from %d\n", rf.me, args.CandidateId)

	// we don't need to check if they've voted? only that the current term is larger?
	if rf.role == 'F' && args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term

		reply.VoteGranted = true

        fmt.Printf("%d just voted for %d\n", rf.me, rf.votedFor)
		rf.unsafeTicks()
		// DEMOTE IF LATER TERM IS FOUND
	} else if (rf.role == 'C' || rf.role == 'L') && rf.currentTerm < args.Term {
		rf.role = 'F'
		rf.isLeader = false
		rf.votesRec = 1

		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term

		reply.VoteGranted = true

        fmt.Printf("%d just voted for %d\n", rf.me, rf.votedFor)
		rf.unsafeTicks()
	} else {
		reply.VoteGranted = false
	}

	rf.mu.Unlock()
}

func (rf *Raft) AppendEntry(args *AppendEntries, res *AppendEntries) {
	rf.mu.Lock()

	// RESTART TICKER FOR LEADER
	if rf.isLeader && args.Term > rf.currentTerm {
        rf.demote(args.Term)
		rf.mu.Unlock()
		go rf.ticker()
		return
	}

	// old leader trying to send message so we don't generate ticks
	if args.Term < rf.currentTerm {
        res.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.unsafeTicks()

	rf.mu.Unlock()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	if rf.me == server {
		return
	}
	rep := RequestVoteReply{}

	ok := rf.peers[server].Call("Raft.RequestVote", args, &rep)

	if ok && rep.VoteGranted {
		rf.mu.Lock()
		rf.votesRec++
		if rf.votesRec >= majorityNum(len(rf.peers)) && rf.role != 'L' { // already leader
			rf.role = 'L'
			rf.isLeader = true
			// start heartbeats
			// ADD A SYNC ONCE FOR THIS??
			rf.mu.Unlock()
			rf.heartbeats()
		} else { // NEED THIS OTHERWISE WHEN LEADER IS DONE RUNNING IT'LL TRY TO UNLOCK AN UNLOCKED LOCK
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) demote(term int) {
    rf.role = 'F'
    rf.isLeader = false
    rf.currentTerm = term
    rf.unsafeTicks()
}

func (rf *Raft) sendBeat(server int, a *AppendEntries) {
	if rf.me == server {
		return
	}

    res := AppendEntries{}

    ok := rf.peers[server].Call("Raft.AppendEntry", a, &res)
    if ok {
        rf.mu.Lock()

        if res.Term > rf.currentTerm {
            rf.demote(res.Term)
            go rf.ticker()
        }

        rf.mu.Unlock()
    }
}

// leader sends heartbeats
func (rf *Raft) heartbeats() {
	a := AppendEntries{}
	rf.mu.Lock()
	a.Term = rf.currentTerm
	a.Server = rf.me
	rf.mu.Unlock()

	for _, leader := rf.GetState(); leader; {
		for i, _ := range rf.peers {
			go rf.sendBeat(i, &a)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func majorityNum(a int) int {
	if a%2 == 0 {
		return a / 2
	} else {
		return a/2 + 1
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Use cautiously
func (rf *Raft) unsafeTicks() {
	ms := 500 + (rand.Int63() % 700)
	// r := rand.New(rand.NewSource(int64(rf.me * 2)))
	// ms := 1000 + (r.Int63() % 500)
	rf.ticks = ms / 30
	rf.rem = ms % 30
}

func (rf *Raft) genTicks() {
	rf.mu.Lock()
	ms := 500 + (rand.Int63() % 700)
	// r := rand.New(rand.NewSource(int64(rf.me * 2)))
	// ms := 1000 + (r.Int63() % 500)
	rf.ticks = ms / 30
	rf.rem = ms % 30
	rf.mu.Unlock()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.role = 'C'
	rf.currentTerm++
	rf.votesRec = 1

	req := RequestVoteArgs{}

	req.Term = rf.currentTerm
	req.CandidateId = rf.me
    fmt.Printf("startElection - server: %v term: %v\n", rf.me, rf.currentTerm)

	for i, _ := range rf.peers {
		go rf.sendRequestVote(i, &req)
	}

	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	// pause for a random amount of time between 50 and 350
	// milliseconds.
	for rf.killed() == false {

		rf.mu.Lock()
		if rf.isLeader {
			rf.mu.Unlock() // this otherwise causes a deadlock
			return         // stop timer if we are the leader
		}

		// Your code here (2A)
		// Check if a leader election should be started.
		if rf.ticks == 0 && rf.rem == 0 {
			// Election timeout
			go rf.startElection()
			rf.unsafeTicks()
		}

		if rf.ticks == 0 {
			rf.rem = 0
			time.Sleep(time.Duration(rf.rem) * time.Millisecond)
		} else {
			rf.ticks--
			time.Sleep(30 * time.Millisecond)
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.genTicks()
	rf.role = 'F'
	rf.votedFor = -1
	rf.votesRec = 1 // always vote for self

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
