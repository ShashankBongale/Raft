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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

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

type serverState int

const (
	follower serverState = iota
	candidate
	leader
	killed
)

type Event struct {
	Command interface{}
	Term    int
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

	currentTerm       int
	currentSeverState serverState

	lastLeaderComTime time.Time

	lastVotedTerm int
	votedFor      int

	//eventLog          []Event
	committedEventLog []Event //should change this name
	applitedEventLog  []Event //We might not need this

	nextIndexForPeers map[int]int

	toBeCommittedEvent    int
	eventReplicationCount map[int]int

	applyCh chan ApplyMsg
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	CandidateTerm                int
	CandidateId                  int
	CandidateCommittedEventCount int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term       int
	LeaderId   int
	NewCommand interface{}

	PreceedingIndex int
	PreceedingTerm  int
	//PreceedingCommand interface{}

	CommittedIndex int
}

type AppendEntriesReply struct {
	CurrentTerm         int
	IsRequestSuccessful bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//rf.currentSeverState
	//fmt.Println("Checking status for server", rf.me)
	var term int = -1
	var isleader bool = false

	// Your code here (2A).

	if !rf.killed() {
		rf.mu.Lock()
		term = rf.currentTerm
		isleader = rf.currentSeverState == leader
		rf.mu.Unlock()
	}

	//fmt.Println("Server", rf.me, "is in leader state:", isleader, "with term", term)
	return term, isleader
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	reply.VoteGranted = false

	rf.mu.Lock()

	committedEventCount := len(rf.committedEventLog)
	/*var lastCommittedLogTerm int = -1

	if committedEventCount > 0 {
		lastCommittedLogTerm = rf.committedEventLog[committedEventCount-1].Term
	}*/

	var isCandidateHasAllInfo bool = false

	//Leader should have upto date info as of this server. This is election restriction in paper
	if rf.currentTerm <= args.CandidateTerm && committedEventCount <= args.CandidateCommittedEventCount {
		isCandidateHasAllInfo = true
	}

	if isCandidateHasAllInfo && rf.lastVotedTerm < args.CandidateTerm {
		reply.VoteGranted = true
		fmt.Println("Server", rf.me, "granting vote for", args.CandidateId, "for term", args.CandidateTerm)
		rf.lastVotedTerm = args.CandidateTerm
		rf.votedFor = args.CandidateId
	}

	rf.mu.Unlock()
}

func (rf *Raft) AppendEntry(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	var currentTerm int
	var currentState serverState

	rf.mu.Lock()

	rf.lastLeaderComTime = time.Now()
	currentTerm = rf.currentTerm
	currentState = rf.currentSeverState

	//fmt.Println("Got append request from server", args.LeaderId, "with term", args.Term, "for server", rf.me, "with term", rf.currentTerm)

	if currentTerm <= args.Term {

		rf.currentTerm = args.Term
		currentTerm = args.Term

		if currentState == candidate || currentState == leader {
			rf.currentSeverState = follower
		}
	}

	reply.CurrentTerm = currentTerm

	if rf.currentSeverState == follower {
		logEntryCount := len(rf.committedEventLog)

		//Consistency check
		if logEntryCount > 0 {
			lastEvent := rf.committedEventLog[logEntryCount-1]

			if lastEvent.Term != args.PreceedingTerm || logEntryCount-1 != args.PreceedingIndex /*|| lastEvent.Command != args.PreceedingCommand*/ {
				reply.IsRequestSuccessful = false

				rf.mu.Unlock()
				return
			}
		}

		//fmt.Println("Server", rf.me, "got command", args.NewCommand)
		reply.IsRequestSuccessful = true

		if args.NewCommand != "" {
			newEvent := Event{Command: args.NewCommand, Term: currentTerm}
			rf.committedEventLog = append(rf.committedEventLog, newEvent)

		}

		if args.CommittedIndex != -1 {
			applyMsg := ApplyMsg{CommandValid: true, Command: rf.committedEventLog[args.CommittedIndex].Command, CommandIndex: args.CommittedIndex + 1}
			//fmt.Println("Follower server sending apply msg", applyMsg)
			rf.applyCh <- applyMsg
		}

	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Println("Sending vote request from", rf.me, "to", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("Sending appen entry request from", rf.me, "to", serverId)
	ok := rf.peers[serverId].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) heartBeatSender() {

	var currentServerState serverState
	var isKilled bool

	rf.mu.Lock()
	currentServerState = rf.currentSeverState
	isKilled = rf.killed()
	rf.mu.Unlock()

	for currentServerState == leader && !isKilled {

		var maxTerm int = -1
		var maxTermLock sync.Mutex
		var wg sync.WaitGroup

		for peerItr := 0; peerItr < len(rf.peers); peerItr++ {
			if peerItr != rf.me {
				wg.Add(1)

				preceedingIndex := -1
				preceedingTerm := -1
				var preceedingCommand interface{}
				var newCmd interface{} = ""
				committedIndex := -1

				rf.mu.Lock()

				if rf.nextIndexForPeers[peerItr] != 0 {
					preceedingIndex = rf.nextIndexForPeers[peerItr] - 1
					preceedingTerm = rf.committedEventLog[preceedingIndex].Term
					preceedingCommand = rf.committedEventLog[preceedingIndex].Command
				}

				if rf.nextIndexForPeers[peerItr] < len(rf.committedEventLog) {
					newCmd = rf.committedEventLog[rf.nextIndexForPeers[peerItr]].Command
				}

				if rf.toBeCommittedEvent > 0 {
					committedIndex = rf.toBeCommittedEvent - 1
				}

				rf.mu.Unlock()

				go func(peerId int, newCmd interface{}, preceedingIndex int, preceedingTerm int, preceedingCmd interface{}, committedIndex int) {

					if newCmd != "" {
						fmt.Println("Server", rf.me, "sending command for index", preceedingIndex+1, "to server", peerId)
					}
					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, NewCommand: newCmd, PreceedingIndex: preceedingIndex, PreceedingTerm: preceedingTerm /*PreceedingCommand: preceedingCmd, */, CommittedIndex: committedIndex}
					reply := AppendEntriesReply{CurrentTerm: -1, IsRequestSuccessful: false}

					ok := rf.sendAppendEntries(peerId, &args, &reply)
					if ok {
						maxTermLock.Lock()
						if reply.CurrentTerm > maxTerm {
							maxTerm = reply.CurrentTerm
						}
						maxTermLock.Unlock()

						if newCmd != "" {
							if !reply.IsRequestSuccessful {
								rf.mu.Lock()

								if rf.nextIndexForPeers[peerId] != 0 {
									rf.nextIndexForPeers[peerId] -= 1
								}

								rf.mu.Unlock()
							} else {
								rf.mu.Lock()
								rf.eventReplicationCount[rf.nextIndexForPeers[peerId]] += 1
								rf.nextIndexForPeers[peerId] += 1
								rf.mu.Unlock()
							}
						}
					}
					wg.Done()
				}(peerItr, newCmd, preceedingIndex, preceedingTerm, preceedingCommand, committedIndex)
			}
		}

		wg.Wait()

		var currentTerm int

		rf.mu.Lock()
		currentTerm = rf.currentTerm
		rf.mu.Unlock()

		if maxTerm == -1 || maxTerm > currentTerm {
			rf.mu.Lock()
			rf.currentTerm = maxTerm
			rf.currentSeverState = follower
			rf.mu.Unlock()
			return
		}

		rf.mu.Lock()
		if len(rf.committedEventLog) > 0 && rf.eventReplicationCount[rf.toBeCommittedEvent] >= int(math.Ceil(float64((len(rf.peers))/2.0))) {

			fmt.Println("Command committed by server", rf.me, "for index", rf.toBeCommittedEvent)
			applyMsg := ApplyMsg{CommandValid: true, Command: rf.committedEventLog[rf.toBeCommittedEvent].Command, CommandIndex: rf.toBeCommittedEvent + 1}

			//fmt.Println("Leader server sending apply msg", applyMsg)

			rf.applyCh <- applyMsg

			rf.toBeCommittedEvent += 1

		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		currentServerState = rf.currentSeverState
		isKilled = rf.killed()
		rf.mu.Unlock()
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
	rf.mu.Lock()
	isLeader = rf.currentSeverState == leader
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	term = rf.currentTerm
	var newEvent Event = Event{Command: command, Term: term}
	rf.committedEventLog = append(rf.committedEventLog, newEvent)
	index = len(rf.committedEventLog) - 1
	rf.eventReplicationCount[index] = 1
	fmt.Println("Got data for index", index, "for server", rf.me)
	rf.mu.Unlock()

	return index + 1, term, isLeader
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

	fmt.Println("Got kill for", rf.me)
	rf.mu.Lock()
	rf.currentSeverState = killed
	rf.currentTerm = -1
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		ms := 200 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		var lastLeaderCommunicationTime time.Time

		if rf.currentSeverState == follower {
			rf.mu.Lock()
			lastLeaderCommunicationTime = rf.lastLeaderComTime
			rf.mu.Unlock()
		} else {
			lastLeaderCommunicationTime = time.Now()
		}

		if time.Now().Sub(lastLeaderCommunicationTime).Milliseconds() >= 100 {

			fmt.Println("Starting election for server", rf.me)

			var currentTerm int
			var committedEventCount int
			rf.mu.Lock()
			rf.currentSeverState = candidate
			rf.currentTerm += 1
			currentTerm = rf.currentTerm
			committedEventCount = len(rf.committedEventLog)
			rf.mu.Unlock()

			//what happens when majority of servers are down? Should that case be handled?
			//start election

			var finished int = 0
			var voteCount int = 1 //candidate votes for itself
			var followerRespLock sync.Mutex

			for peerItr := 0; peerItr < len(rf.peers); peerItr++ {

				if peerItr != rf.me {

					go func(peerId int) {

						args := RequestVoteArgs{CandidateTerm: currentTerm, CandidateId: rf.me, CandidateCommittedEventCount: committedEventCount}
						reply := RequestVoteReply{VoteGranted: false}
						ok := rf.sendRequestVote(peerId, &args, &reply)

						followerRespLock.Lock()
						if ok {

							if reply.VoteGranted {
								voteCount += 1
							}
						}

						finished += 1
						followerRespLock.Unlock()

					}(peerItr)
				}
			}

			var currentFinished int
			var currentState serverState
			var termBeforeElection int

			followerRespLock.Lock()
			currentFinished = finished
			followerRespLock.Unlock()

			rf.mu.Lock()
			currentState = rf.currentSeverState
			currentTerm = rf.currentTerm
			termBeforeElection = rf.currentTerm
			rf.mu.Unlock()

			//we just need half of the servers to send votes to get the majority
			for currentFinished < int(math.Ceil(float64((len(rf.peers)-1)/2.0))) && currentState == candidate && currentTerm == termBeforeElection {
				time.Sleep(10 * time.Millisecond)

				followerRespLock.Lock()
				currentFinished = finished
				followerRespLock.Unlock()

				rf.mu.Lock()
				currentState = rf.currentSeverState
				currentTerm = rf.currentTerm
				rf.mu.Unlock()

			}

			//continue election only if this server is still candidate
			if currentState == candidate && currentTerm == termBeforeElection {
				followerRespLock.Lock()

				var majorityCont int = int(math.Ceil(float64(len(rf.peers)) / 2.0))

				if voteCount >= majorityCont {
					rf.mu.Lock()
					rf.currentSeverState = leader
					rf.mu.Unlock()

					//fmt.Println("Received", voteCount, "votes out of", len(rf.peers), "for server", rf.me, "making it leader. Majority count", majorityCont)

					go rf.heartBeatSender()

				} else {
					rf.mu.Lock()
					rf.currentSeverState = follower
					rf.mu.Unlock()
				}
				followerRespLock.Unlock()
			}

		} else {
			time.Sleep(10 * time.Millisecond)
		}
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.currentSeverState = follower
	rf.lastLeaderComTime = time.Time{}
	rf.votedFor = -1
	rf.lastVotedTerm = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndexForPeers = make(map[int]int)
	rf.eventReplicationCount = make(map[int]int)

	for peerItr := 0; peerItr < len(peers); peerItr++ {
		rf.nextIndexForPeers[peerItr] = 0
	}

	rf.toBeCommittedEvent = 0
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
