package raft

import (
	"fmt"
	"math/rand"
	"raft/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type logEntity struct {
	Term    int
	Command interface{}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	VoteCounted bool
}

type ServerState int

const (
	Unknown ServerState = iota
	Follower
	Candidate
	Leader
)

type AppendEntriesReq struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	LogEntries        []logEntity
	LeaderCommitIndex int
	AppendingIndex    int
}

type AppendEntriesRepl struct {
	IsSuccess bool
	GotReply  bool
}

// A Go object implementing a single raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a raft server must maintain.

	currentTerm int
	votedFor    int
	votedTerm   int
	log         []logEntity
	commitIndex int
	lastApplied int

	currentState ServerState

	electionTimeOutOffset int

	leaderLastCommunicationTime time.Time

	electionStartTime time.Time

	majorityCount int

	nextIndex  []int
	matchIndex []int

	applyCh chan ApplyMsg
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.currentState == Leader

	return term, isleader
}

// save raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	currLastLogTerm := 0
	currLastLogIndex := len(rf.log)

	if currLastLogIndex > 0 {
		currLastLogTerm = rf.log[currLastLogIndex-1].Term
	}

	//Follower votes only if candidate is as updated as follower or has more data
	if currLastLogTerm == args.LastLogTerm {

		if currLastLogIndex <= args.LastLogIndex {

			reply.VoteGranted = true
			rf.votedTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.leaderLastCommunicationTime = time.Now()
			fmt.Println(rf.me, " Voted for ", args.CandidateId)
			return
		}

	} else if currLastLogTerm < args.LastLogTerm {

		reply.VoteGranted = true
		rf.votedTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.leaderLastCommunicationTime = time.Now()
		fmt.Println(rf.me, " Voted for ", args.CandidateId)
		return
	}

	fmt.Println(rf.me, " Rejecting vote for ", args.CandidateId)

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesReq, reply *AppendEntriesRepl) {

	fmt.Println(rf.me, "got append request from ", args.LeaderId)

	//Append entry is sent only by leader
	reply.IsSuccess = false

	rf.mu.Lock()

	if rf.currentTerm > args.Term && rf.currentState != Candidate {

		fmt.Println(rf.me, " rejecting ", args.LeaderId)
		rf.mu.Unlock()
		return
	}

	var toBeAppenededIndex int = args.AppendingIndex
	var prevIndex int = toBeAppenededIndex - 1

	if args.AppendingIndex <= rf.commitIndex {

		if len(args.LogEntries) > 0 {
			if rf.log[args.AppendingIndex] == args.LogEntries[0].Command {

				//duplicate data => so just accept it
				reply.IsSuccess = true
				rf.leaderLastCommunicationTime = time.Now()
				rf.currentTerm = args.Term
				rf.currentState = Follower

			} else if rf.log[args.AppendingIndex].Term < args.LogEntries[0].Term {

				//new data with higher term leader so just accept it
				//What if this data is committed?

				rf.leaderLastCommunicationTime = time.Now()
				rf.currentTerm = args.Term
				rf.currentState = Follower

				rf.log[args.AppendingIndex] = args.LogEntries[0]
				rf.lastApplied = args.AppendingIndex

				rf.log = rf.log[:rf.lastApplied+1] //deleting all entries after this

				prevCommitIndex := rf.commitIndex

				rf.commitIndex = rf.lastApplied
				if args.LeaderCommitIndex > rf.commitIndex {

					if args.LeaderCommitIndex <= rf.lastApplied {
						rf.commitIndex = args.LeaderCommitIndex
					} else {
						rf.commitIndex = rf.lastApplied
					}
				}

				if prevCommitIndex != rf.commitIndex {

					fmt.Println("Committing index ", rf.commitIndex, " with data ", rf.log[rf.commitIndex].Command, " by ", rf.me)

					var msg ApplyMsg = ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex].Command, CommandIndex: rf.commitIndex + 1}

					rf.applyCh <- msg
				}

			}

		}

		rf.mu.Unlock()
		return
	}

	//Log matching condition
	if prevIndex < len(rf.log) && prevIndex >= 0 {

		if rf.log[prevIndex].Term != args.PrevLogTerm {

			rf.leaderLastCommunicationTime = time.Now()
			rf.mu.Unlock()

			return
		}
	} else if prevIndex != -1 {

		rf.mu.Unlock()
		return
	}

	rf.leaderLastCommunicationTime = time.Now()
	rf.currentTerm = args.Term

	rf.currentState = Follower
	if len(args.LogEntries) > 0 {

		fmt.Println(rf.me, " Recved ", args.LogEntries[0].Command)

		var logObj logEntity = args.LogEntries[0]
		var currentIndex int = args.PrevLogIndex + 1

		if len(rf.log) <= currentIndex {

			rf.log = append(rf.log, logObj)
			rf.lastApplied = len(rf.log) - 1
		} else {

			rf.log[currentIndex] = logObj //overwriting the log index
			rf.lastApplied = currentIndex
		}
	}

	prevCommitIndex := rf.commitIndex

	if args.LeaderCommitIndex > rf.commitIndex {

		if args.LeaderCommitIndex <= rf.lastApplied {
			rf.commitIndex = args.LeaderCommitIndex
		} else {
			rf.commitIndex = rf.lastApplied
		}
	}

	if prevCommitIndex != rf.commitIndex {

		fmt.Println("Committing index ", rf.commitIndex, " with data ", rf.log[rf.commitIndex].Command, " by ", rf.me)

		var msg ApplyMsg = ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex].Command, CommandIndex: rf.commitIndex + 1}

		rf.applyCh <- msg
	}

	rf.mu.Unlock()

	reply.IsSuccess = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesReq, reply *AppendEntriesRepl) bool {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	reply.GotReply = true

	if ok {

		if reply.IsSuccess && len(args.LogEntries) > 0 {

			rf.mu.Lock()

			rf.nextIndex[server] += 1
			rf.matchIndex[server] = args.PrevLogIndex + 1

			toBeCommittedIndex := rf.commitIndex + 1

			majorityCount := 0

			for peerItr := 0; peerItr < len(rf.matchIndex); peerItr++ {

				if rf.matchIndex[peerItr] >= toBeCommittedIndex && toBeCommittedIndex < len(rf.log) {

					majorityCount += 1
				}
			}

			if majorityCount >= rf.majorityCount {

				rf.commitIndex = toBeCommittedIndex

				fmt.Println("Committing index ", rf.commitIndex, " with data ", rf.log[rf.commitIndex].Command, " by leader ", rf.me)
				var msg ApplyMsg = ApplyMsg{CommandValid: true, Command: rf.log[rf.commitIndex].Command, CommandIndex: rf.commitIndex + 1}

				rf.applyCh <- msg

			}

			rf.mu.Unlock()

		} else if !reply.IsSuccess && len(args.LogEntries) > 0 {

			rf.mu.Lock()

			if rf.nextIndex[server] != 0 {
				rf.nextIndex[server] -= 1
			}

			rf.mu.Unlock()
		}
	}

	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the raft log, since the leader
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
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.currentState == Leader

	if !isLeader || rf.killed() {
		return index, term, isLeader
	}

	term = rf.currentTerm

	var logObj logEntity = logEntity{Term: rf.currentTerm, Command: command}

	rf.log = append(rf.log, logObj)

	rf.matchIndex[rf.me] = len(rf.log) - 1

	index = len(rf.log)

	for peerItr := 0; peerItr < len(rf.nextIndex); peerItr++ {

		rf.nextIndex[peerItr] = len(rf.log) - 1
	}

	fmt.Println("Leader ", rf.me, " got ", command, " for index ", index-1, "for term ", term)

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

	rf.mu.Lock()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.currentState = Follower
	rf.votedTerm = -1
	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.log = nil
	rf.matchIndex = nil
	rf.nextIndex = nil

	rf.mu.Unlock()
	atomic.StoreInt32(&rf.dead, 1)
	fmt.Println(rf.me, " is killed")
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) SendHeartBeat() {

	for true {

		ss := Unknown
		currTerm := 0
		nextIndex := 0

		rf.mu.Lock()

		ss = rf.currentState
		currTerm = rf.currentTerm

		rf.mu.Unlock()

		if ss != Leader {
			return
		}

		for peerItr := 0; peerItr < len(rf.peers); peerItr++ {

			if peerItr == rf.me {
				continue
			}

			rf.mu.Lock()

			nextIndex = rf.nextIndex[peerItr]

			var logToSend []logEntity

			var prevLogIndex int = -1
			var prevLogTerm int = 0

			if len(rf.log) > 0 {

				prevLogIndex = nextIndex - 1

				if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
					prevLogTerm = rf.log[prevLogIndex].Term
				}

				fmt.Println("Leader ", rf.me, " log length ", len(rf.log))

				if nextIndex < len(rf.log) {

					logObj := rf.log[nextIndex]
					logToSend = append(logToSend, logObj) //sending only one as of now. Need to optimise this.
				}
			}

			appendEntryReq := AppendEntriesReq{Term: currTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, LogEntries: logToSend, LeaderCommitIndex: rf.commitIndex, AppendingIndex: nextIndex}

			fmt.Println(rf.me, " Sending ", peerItr, appendEntryReq)
			rf.mu.Unlock()

			appendEntryRepl := AppendEntriesRepl{IsSuccess: false, GotReply: false}

			if len(appendEntryReq.LogEntries) > 0 {
				fmt.Println(rf.me, "sending ", appendEntryReq.LogEntries[0].Command, " to ", peerItr, " for term ", rf.currentTerm)
			}

			go rf.sendAppendEntries(peerItr, &appendEntryReq, &appendEntryRepl)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) PerformElection() {

	fmt.Println("Performing election by ", rf.me)

	rf.mu.Lock()

	rf.currentTerm += 1
	rf.currentState = Candidate
	rf.votedFor = rf.me
	rf.votedTerm = rf.currentTerm

	currentLastLogIndex := 0
	currentLastTerm := 0

	if len(rf.log) > 0 {

		currentLastLogIndex = len(rf.log)
		currentLastTerm = rf.log[currentLastLogIndex-1].Term
	}

	var voteRequestArg RequestVoteArgs = RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: currentLastLogIndex, LastLogTerm: currentLastTerm}

	rf.electionStartTime = time.Now()
	var voteRecv int = 1

	rf.mu.Unlock()

	peerItr := 0

	var peerVoteReplies map[int]*RequestVoteReply = make(map[int]*RequestVoteReply)

	for peerItr < len(rf.peers) {

		if peerItr == rf.me {
			peerItr += 1
			continue
		}

		voteReplyArg := new(RequestVoteReply)

		voteReplyArg.Term = 0
		voteReplyArg.VoteGranted = false
		voteReplyArg.VoteCounted = false

		peerVoteReplies[peerItr] = voteReplyArg

		term := 0

		rf.mu.Lock()
		term = rf.currentTerm
		rf.mu.Unlock()

		fmt.Println(rf.me, " asking vote to ", peerItr, " for term ", term)
		go rf.sendRequestVote(peerItr, &voteRequestArg, peerVoteReplies[peerItr])

		peerItr += 1
	}

	fmt.Println("Counting votes by ", rf.me)

	for true {

		var ss ServerState = Unknown

		rf.mu.Lock()
		ss = rf.currentState
		rf.mu.Unlock()

		if ss != Candidate {
			return
		}

		for _, repl := range peerVoteReplies {

			//fmt.Println("Checking vote reply for ", key)

			if repl.VoteCounted == false && repl.VoteGranted == true {

				//fmt.Println(rf.me, " received by ", key)
				voteRecv += 1
				repl.VoteCounted = true
			}

			if voteRecv >= rf.majorityCount {

				rf.mu.Lock()

				peerItr := 0

				for peerItr < len(rf.peers) {

					rf.matchIndex[peerItr] = -1

					if len(rf.log) == 0 {
						rf.nextIndex[peerItr] = 0
					} else {
						rf.nextIndex[peerItr] = len(rf.log) - 1
					}

					peerItr += 1
				}

				rf.currentState = Leader
				rf.mu.Unlock()

				fmt.Println("New leader ", rf.me, " for term ", rf.currentTerm)

				go rf.SendHeartBeat()

				return

			}

			time.Sleep(1 * time.Millisecond)
		}
	}

}

func (rf *Raft) CheckForLeaderContact() {

	fmt.Println("Performing leader contact check on ", rf.me)

	for true {

		isKilled := rf.killed()

		if isKilled == true {
			return
		}

		var myState ServerState = Unknown

		rf.mu.Lock()
		myState = rf.currentState
		rf.mu.Unlock()

		if myState == Leader {

			time.Sleep(1 * time.Millisecond)
			continue
		}

		if myState == Candidate {

			rf.mu.Lock()
			lastElectionStartTime := rf.electionStartTime
			rf.mu.Unlock()

			timeAfterElection := time.Now().Sub(lastElectionStartTime)

			if timeAfterElection > (time.Duration(rf.electionTimeOutOffset) * time.Millisecond) {

				go rf.PerformElection() //what will happen to candiate with prev election => the older PerformElection thread will eventually die

			}

			time.Sleep(1 * time.Millisecond)
			continue

		}

		var leaderLastContact time.Time

		rf.mu.Lock()
		leaderLastContact = rf.leaderLastCommunicationTime
		rf.mu.Unlock()

		timeDiff := time.Now().Sub(leaderLastContact)

		//timeOutInSecs := 1 * time.Second

		//Need to check this logic
		if timeDiff > time.Duration(rf.electionTimeOutOffset)*time.Millisecond /*+timeOutInSecs*/ {

			go rf.PerformElection()

		}

		time.Sleep(1 * time.Millisecond)
	}

}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.currentState = Follower
	rf.votedTerm = -1
	rf.electionTimeOutOffset = rand.Intn(1000-300) + (rf.me+1)*300
	rf.commitIndex = -1
	rf.lastApplied = -1

	fmt.Println("Election timeout offset set to ", rf.electionTimeOutOffset, " for ", rf.me)
	rf.majorityCount = (len(rf.peers) / 2) + 1
	rf.leaderLastCommunicationTime = time.Now() //starting timer. Eventually one candidate will timeout and become candidate.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	peerItr := 0

	for peerItr < len(rf.peers) {

		rf.matchIndex = append(rf.matchIndex, -1)
		rf.nextIndex = append(rf.nextIndex, len(rf.log))

		peerItr += 1
	}

	rf.applyCh = applyCh

	go rf.CheckForLeaderContact()

	return rf
}
