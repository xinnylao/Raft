package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"cs350/labgob"
	"cs350/labrpc"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor      int
	log           []CommandLogEntry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	role          string //follower, candidate, or leader
	numVotes      int
	electionTimer *time.Timer
	resetTimer    chan struct{}
	applyCh       chan ApplyMsg
}

type CommandLogEntry struct {
	Term    int
	Command interface{}
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (4A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	if rf.role == "leader" {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4B).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []CommandLogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("decode not null")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	CurrentTerm int
	VoteGranted bool
}

type AppendEntriesArg struct {
	LeaderTerm   int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []CommandLogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	CurrentTerm   int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func GrantVote(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.VoteGranted = true
	rf.role = "follower"
	rf.votedFor = args.CandidateID
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}
	rf.persist()
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (4A, 4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.CurrentTerm = rf.currentTerm
	reply.VoteGranted = false
	// fmt.Println(args.CandidateID, "args.Term is ", args.Term, " and ", rf.me, " rf.currentTerm is ", rf.currentTerm)
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = "follower"
		rf.persist()
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		if len(rf.log) == 0 {
			GrantVote(rf, args, reply)
			return
		}
		if rf.log[len(rf.log)-1].Term < args.LastLogTerm {
			GrantVote(rf, args, reply)
			return
		}
		if rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log) <= args.LastLogIndex {
			GrantVote(rf, args, reply)
			return
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	//fmt.Println("follower ", rf.me, " has log", rf.log)
	rf.resetTimer <- struct{}{}
	rf.mu.Lock()
	if rf.currentTerm < args.LeaderTerm {
		rf.currentTerm = args.LeaderTerm
		rf.role = "follower"
		rf.votedFor = -1
		rf.persist()
	}
	rf.mu.Unlock()
	rf.mu.RLock()
	reply.CurrentTerm = rf.currentTerm
	reply.Success = true
	reply.ConflictIndex = 1
	rf.mu.RUnlock()
	rf.mu.Lock()
	//fmt.Println(rf.me, "'s term is ", rf.currentTerm)
	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		// fmt.Println(rf.me, " rejected because args.LeaderTerm is < than rf.currentTerm ", args.LeaderTerm, rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(rf.log) {
			reply.Success = false
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = 0
			//fmt.Println("args.PrevLogIndex", args.PrevLogIndex)
			//fmt.Println("(len(rf.log)-1)", len(rf.log)-1)
			//fmt.Println("follower ", rf.me, " has log", rf.log)
			// fmt.Println("rf.log[args.PrevLogIndex].Term", rf.log[args.PrevLogIndex].Term)
			// fmt.Println("args.PrevLogTerm", args.PrevLogTerm)
			rf.mu.Unlock()
			return
		} else if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.Success = false
			reply.ConflictTerm = rf.log[args.PrevLogIndex-1].Term
			for i, entry := range rf.log {
				if entry.Term == reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					//fmt.Println("conflictIndex is", reply.ConflictIndex)
					break
				}
			}
			rf.mu.Unlock()
			return
		}
	}
	if len(rf.log) > args.PrevLogIndex {
		if args.PrevLogIndex < 0 {
			rf.log = []CommandLogEntry{}
		} else {
			rf.log = rf.log[:args.PrevLogIndex]
		}
	}
	rf.log = append(rf.log, args.Entries...)
	if args.PrevLogTerm == 0 {
		rf.log = args.Entries
	}
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log)
		}
	}
	start := rf.lastApplied
	//fmt.Println(rf.me, "'s commitIndex is", rf.commitIndex)
	for i := start; i < rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i + 1,
		}
		rf.applyCh <- msg
		rf.lastApplied++
	}
	// fmt.Println("follower ", rf.me, " has log length", len(rf.log))
	//fmt.Println("follower ", rf.me, " has log", rf.log)
	rf.mu.Unlock()
}

// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
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
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//fmt.Println(server, " granted vote ", reply.VoteGranted)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	role := rf.role
	if ok && reply.VoteGranted && role == "candidate" {
		rf.numVotes++
		if rf.numVotes > (len(rf.peers) / 2) {
			rf.role = "leader"
			rf.electionTimer.Stop()
			rf.nextIndex = make([]int, len(rf.peers))
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log) + 1
			}
			rf.matchIndex = make([]int, len(rf.peers))
			rf.matchIndex[rf.me] = len(rf.log)
			go rf.ticker()
			//fmt.Println(rf.me, " is set to leader")
			return ok
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArg, reply *AppendEntriesReply) bool {
	// fmt.Println("leader has log length", len(rf.log))
	//fmt.Println("leader", rf.me, "has log", rf.log)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && len(args.Entries) > 0 && reply.Success {
		rf.matchIndex[server] = len(rf.log)
		//fmt.Println("matchIndex is ", rf.matchIndex)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		if rf.nextIndex[server] == 0 {
			rf.nextIndex[server] = 1
		}
		//fmt.Println(server, " added to nextIndex ", len(args.Entries))
		//fmt.Println("args.Entries sent to", server, "is", args.Entries)
	}
	copiedMatchIndex := make([]int, len(rf.matchIndex))
	copy(copiedMatchIndex, rf.matchIndex)
	sort.Ints(copiedMatchIndex)
	oldIndex := rf.commitIndex
	rf.commitIndex = copiedMatchIndex[len(copiedMatchIndex)/2]
	for rf.commitIndex > 0 && rf.commitIndex > oldIndex && rf.log[rf.commitIndex-1].Term != rf.currentTerm {
		rf.commitIndex--
	}
	start := rf.lastApplied
	for i := start; i < rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i + 1,
		}
		rf.applyCh <- msg
		rf.lastApplied++
	}
	if ok && len(args.Entries) > 0 && !reply.Success {
		if reply.CurrentTerm > rf.currentTerm {
			rf.role = "follower"
			rf.votedFor = -1
			rf.persist()
		} else {
			isHere := false
			for i, entry := range rf.log {
				if entry.Term == reply.ConflictTerm {
					rf.nextIndex[server] = i + 1
					isHere = true
				}
			}
			if isHere {
				rf.nextIndex[server]++
				if rf.nextIndex[server] == 0 {
					rf.nextIndex[server] = 1
				}
				// if rf.nextIndex[server] > rf.nextIndex[rf.me] {
				// 	rf.nextIndex[server] = rf.nextIndex[rf.me]
				// }
				fmt.Println("optimization")
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
				if rf.nextIndex[server] == 0 {
					rf.nextIndex[server] = 1
				}
				fmt.Println("optimization")
			}
			//fmt.Println("nextIndex is", rf.nextIndex)
			// if rf.nextIndex[server] > reply.LogLength+1 {
			// 	rf.nextIndex[server] = reply.LogLength + 1
			// } else {
			// 	rf.nextIndex[server]--
			// }
			//fmt.Println("RF.NEXTINDEX", server, " IS REDUCE ", rf.nextIndex)
		}
	}
	//fmt.Println("commitindex is ", rf.commitIndex)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log) + 1
	term := rf.currentTerm
	var isLeader bool
	isTrue := (rf.role == "leader")
	if isTrue {
		isLeader = true
	} else {
		isLeader = false
		return index, term, isLeader
	}

	// Your code here (4B).
	newLog := CommandLogEntry{rf.currentTerm, command}
	rf.log = append(rf.log, newLog)
	rf.persist()
	rf.matchIndex[rf.me]++
	rf.nextIndex[rf.me]++
	if rf.nextIndex[rf.me] == 0 {
		rf.nextIndex[rf.me] = 1
	}
	// fmt.Println("leader log is ", rf.log)
	//fmt.Println("matchIndex is", rf.matchIndex)
	//fmt.Println("nextIndex is", rf.nextIndex)
	//fmt.Println("start returned ", index, term, isLeader)
	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	//fmt.Println(rf.me, " started ticker")
	for !rf.killed() {
		//fmt.Println(rf.me, " is in loop as ", rf.role)

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		/* time out for 250 to 400 miliseconds
		will probably use 120 ms heartbeats*/
		rf.mu.RLock()
		role := rf.role
		peers := rf.peers
		me := rf.me
		rf.mu.RUnlock()
		if role == "leader" {
			//fmt.Println(rf.me, " sends heartbeat")
			// fmt.Println("leader len(rf.log)-1", len(rf.log)-1)
			for i := range peers {
				if i != me {
					rf.mu.RLock()
					args := AppendEntriesArg{
						LeaderTerm:   rf.currentTerm,
						LeaderID:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  0,
						Entries:      []CommandLogEntry{},
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.RUnlock()
					go func(server int) {
						rf.mu.Lock()
						if rf.nextIndex[server] >= 2 {
							//fmt.Println("nextIndex is ", rf.nextIndex)
							//fmt.Println("matchIndex is", rf.matchIndex)
							args.PrevLogTerm = rf.log[rf.nextIndex[server]-2].Term
						} else if len(rf.log) > 0 {
							args.PrevLogTerm = rf.log[0].Term
						}
						if args.PrevLogTerm == 0 {
							args.Entries = rf.log
						} else if len(rf.log) >= rf.nextIndex[server] {
							if rf.nextIndex[server] <= 0 {
								args.Entries = rf.log[0:]
							} else {
								args.Entries = rf.log[rf.nextIndex[server]-1:]
							}
							// fmt.Println("args.Entries length sent to ", server, " is ", len(args.Entries))
							//fmt.Println("args.Entries sent to ", server, " is ", args.Entries)
						}
						//fmt.Println("prevLogIndex is ", rf.nextIndex[i]-1)
						rf.mu.Unlock()
						rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
					}(i)
				}
			}
			time.Sleep(time.Duration(120) * time.Millisecond)
		} else {
			rf.electionTimer.Reset(time.Duration(rand.Intn(151)+250) * time.Millisecond)
			select {
			case <-rf.electionTimer.C:
				// fmt.Println(rf.me, " election timer ran out")
				rf.mu.Lock()
				rf.role = "candidate"
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()
				rf.numVotes = 1
				rf.mu.Unlock()
				rf.mu.RLock()
				args := RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateID: rf.me,
				}
				args.LastLogIndex = len(rf.log)
				if len(rf.log) > 0 {
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
				}
				//fmt.Println(rf.me, " should start election")
				//fmt.Println(rf.me, " sends vote requests")
				for i := range rf.peers {
					if i != rf.me && rf.role == "candidate" {
						go func(server int) {
							rf.sendRequestVote(server, &args, &RequestVoteReply{})
						}(i)
					}
				}
				rf.mu.RUnlock()
			case <-rf.resetTimer:
				//fmt.Println(rf.me, " 's timer is reset")
				rf.mu.Lock()
				if rf.role == "candidate" {
					rf.role = "follower"
				}
				rf.electionTimer.Reset(time.Duration(rand.Intn(151)+250) * time.Millisecond)
				rf.mu.Unlock()
			}
		}
	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
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

	// Your initialization code here (4A, 4B).
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	var newLog []CommandLogEntry
	rf.log = newLog
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = "follower"
	// fmt.Println(rf.me, " set rf.role to ", rf.role)
	rf.numVotes = 0
	rf.electionTimer = time.NewTimer(time.Duration(rand.Intn(151)+250) * time.Millisecond)
	rf.resetTimer = make(chan struct{})

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
