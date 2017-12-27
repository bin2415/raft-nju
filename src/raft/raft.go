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
	"labrpc"
	"encoding/gob"
	"bytes"
	"time"
	"fmt"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

const BroadcastTime =  time.Duration(50) * time.Millisecond

const MinimumElectionTimeoutMS = 500
const MaximumElectionTimeoutMS = 2 * MinimumElectionTimeoutMS

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

//Added Struct Entry
type Entry struct{
	Index int
	Term int
	Command interface{}
}
//End Added

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

	//Persistent state on all servers
	currentTerm int
	votedFor int
	log []Entry

	//Volatile State on all servers
	commitIndex int
	lastApplied int
	//lastLogTerm int

	//Volatile state on leaders
	nextIndex []int
	matchIndex []int
	state int // state = 0 is follwer , state = 1 is candidate, state = 2 is leader

	//timer
	electionTime *time.Timer

	// when the server is candidate, but if it receive a AppendEntries RPC that
	// term is up-to-date, the candidate should convert to follower
	candiConvertToFollower chan int

	// when the server is leader, but if it receive a AppendEntries RPC that
	// term is up-to-date, the candidate should convert to follower
	leaderConvertToFollower chan int

	// heartbeat channel
	heartBeatChan chan int

	commitChan chan int


}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm

	if rf.state == 2 {
		isleader = true
	}else{
		isleader = false
	}
	return term, isleader
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

//AppendEntries RPC arguments structure
type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PervLogIndex int
	PrevLogTerm int
	Entries []Entry
	LeaderCommit int
}

//AppendEntries RPC reply structure
type AppendEntriesReply struct{
	Term int
	Success bool
	NextIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()

	defer rf.mu.Unlock()
	converFollower := false
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		converFollower = true
	}

	reply.Term = rf.currentTerm
	// if rf is a leader, and haven't been convert to follower, it should deny
	if rf.state == 2 && !converFollower{
		reply.VoteGranted = false
		return
	}

	if converFollower && rf.state == 1{
		rf.candiConvertToFollower <- 1
	}
	if converFollower && rf.state == 2{
		//fmt.Println("Convert to follower vote")
		rf.leaderConvertToFollower <- 1
	}


	//the candidate server is not at least as up-to-date as receriver's log or the server is a leader
	if (args.LastLogTerm < rf.log[len(rf.log)-1].Term)||
		((args.LastLogTerm == rf.log[len(rf.log)-1].Term)&&args.LastLogIndex < rf.log[len(rf.log)-1].Index){
		reply.VoteGranted = false
		rf.persist()
	}else if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		//vote to the candidateId
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.resetElectionTimeout()
	}
}

/*
* AppendEntries RPC handler.
* Just implement the heatbeat function...
* To be done: the whole function
 */
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply){

	rf.mu.Lock()
	reply.Success = false
	defer rf.mu.Unlock()
	//defer rf.persist()
	//if the request's term is older than rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.log[len(rf.log) - 1].Index + 1
		return
	}

	//if the rf.state == candidate, then let it returns to a follower state
	if rf.state == 1 && args.LeaderId != rf.me && args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		rf.candiConvertToFollower <- 1
		rf.persist()
	}

	if rf.state == 2 && args.LeaderId != rf.me && args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		rf.leaderConvertToFollower <- 1
		rf.persist()
	}

	//reset state
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.resetElectionTimeout()


	reply.Term = rf.currentTerm

	//term := rf.log[len(rf.log) - 1].Term
	index := rf.log[len(rf.log) - 1].Index

	if args.PervLogIndex > index{
		reply.NextIndex = index + 1
		return
	}

	indexZero := rf.log[0].Index

	if args.PervLogIndex > indexZero{
		term := rf.log[args.PervLogIndex - indexZero].Term
		if args.PrevLogTerm != term{
			for i := args.PervLogIndex - 1; i >= indexZero; i--{
				if rf.log[i-indexZero].Term != term{
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}

	if args.PervLogIndex >= indexZero{
		rf.log = rf.log[: args.PervLogIndex + 1 - indexZero]
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		reply.Success = true
		reply.NextIndex = rf.log[len(rf.log)-1].Index
		//fmt.Println(rf.log)
	}

	if args.LeaderCommit > rf.commitIndex{
		last := rf.log[len(rf.log)-1].Index
		if args.LeaderCommit > last{
			rf.commitIndex = last
		}else{
			rf.commitIndex = args.LeaderCommit
		}

		rf.commitChan <- 1
		//rf.persist()
	}

	return
}

func getElectionTimeOut() time.Duration{
	randNum := rand.Intn(int(MaximumElectionTimeoutMS - MinimumElectionTimeoutMS))
	d := int(MinimumElectionTimeoutMS) + randNum
	return time.Duration(d) * time.Millisecond
}

func (rf *Raft) resetElectionTimeout(){
	rf.electionTime.Reset(getElectionTimeOut())
}

//For follower state
func (rf *Raft) followerHandle(){
	for{
		select{
		case <- rf.electionTime.C:
			rf.currentTerm++
			//fmt.Println("Current server %d, current term %d", rf.me, rf.currentTerm)
			rf.state = 1
			rf.votedFor = -1
			rf.resetElectionTimeout()
			rf.persist()
			return
		}
	}

}

//For candidate state
func (rf *Raft) candidateHandle(){
	//set its vote to itself
	rf.votedFor = rf.me
	rf.persist()

	votedNum := 1

	successLeader := make(chan int)
	serverNum := len(rf.peers)
	requestVoteargs := RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term}
	//var reply *RequestVoteReply
	go func(){
		replyChan := make(chan *RequestVoteReply, 10)
		//var reply *RequestVoteReply
		for i:= 0; i < serverNum; i++{
			if i != rf.me{
				// go 并发访问requestVote
				go func(i int){
					var rep *RequestVoteReply
					ok := rf.peers[i].Call("Raft.RequestVote", requestVoteargs, &rep)
					if ok{
						//将返回的结果传到reply channel中
						replyChan <- rep
					}
					return
				}(i)

			}
		} //end for i:=0

		for{
			reply := <-replyChan
			if reply.Term > rf.currentTerm{
				rf.currentTerm = reply.Term
				rf.state = 0
				rf.votedFor = -1
				rf.persist()
				rf.candiConvertToFollower <- 1
				return
			}
			if reply.VoteGranted{
				votedNum += 1
				if votedNum > serverNum / 2{
					successLeader <- 1
					return
				}
			}

		}

	}() //end go func

	select{

	case <- successLeader: //become the leader
		rf.mu.Lock()
		rf.state = 2 // change state to the leader
		rf.votedFor = -1
		rf.persist()
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i:= range rf.peers{
			rf.nextIndex[i] = rf.log[len(rf.log) - 1].Index + 1
			rf.matchIndex[i] = 0
		}

		rf.mu.Unlock()

		//fmt.Println("Become leader", rf.me)
		return
	case <- rf.electionTime.C:
		rf.resetElectionTimeout()
		rf.currentTerm++
		rf.votedFor = -1
		rf.persist()
		return
	case <- rf.candiConvertToFollower:
		rf.state = 0 //change state to the follower
		//rf.persist()
		return
	}

}

//For leader state
func (rf *Raft) leaderHandle(){

	numPeers := len(rf.peers)
	//initialize the server's nextIndex and matchIndex
	rf.mu.Lock()
	rf.nextIndex = make([]int, numPeers)
	rf.matchIndex = make([]int, numPeers)
	for i := 0; i < numPeers; i++{
		rf.nextIndex[i] = rf.lastApplied + 1
		rf.matchIndex[i] = 0
	}
	rf.persist()
	rf.mu.Unlock()
	//end initialization

	//Timer heartbeat
	heartBeat := time.NewTicker(BroadcastTime)
	defer heartBeat.Stop()
	//heartBeatChan := make(chan int)

	go func(){
		for _ = range heartBeat.C{
			rf.heartBeatChan <- 1
		}
	}()
	for{
		select{
		case <- rf.heartBeatChan:
			peersNum := len(rf.peers)
			baseIndex := rf.log[0].Index
			//args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.lastApplied, rf.lastLogTerm, nil, 0}
			//responses := make(chan int, peersNum-1)
			N := rf.commitIndex
			last := rf.log[len(rf.log)-1].Index
			for i := rf.commitIndex + 1; i <= last; i++{
				num := 1
				for j := range rf.peers{
					if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].Term == rf.currentTerm{
						num++
					}
				}
				if 2*num > len(rf.peers){
					N = i
				}
			}

			if N != rf.commitIndex{
				//fmt.Println("Commited", N)
				rf.commitIndex = N
				rf.commitChan <- 1

			}

			for i := 0; i < peersNum; i++{
				if i != rf.me {
					var reply *AppendEntriesReply
					//var entries	[]Entry
					var args AppendEntriesArgs
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.PervLogIndex = rf.nextIndex[i] -1
					if args.PervLogIndex > len(rf.log)-1{
						args.PervLogIndex = len(rf.log)-1
					}
					args.PrevLogTerm = rf.log[args.PervLogIndex].Term
					args.LeaderCommit = rf.commitIndex
					if rf.nextIndex[i] > baseIndex{
						args.Entries = make([]Entry, len(rf.log[args.PervLogIndex + 1: ]))
						copy(args.Entries, rf.log[args.PervLogIndex + 1: ])
					}
					go func(i int, args AppendEntriesArgs){
						ok := rf.peers[i].Call("Raft.AppendEntries", args, &reply)
						if ok{
							//if reply.Term > rf.currentTerm{
							//	rf.leaderConvertToFollower <- 1
							//	return
							//}
							if reply.Success{
								if len(args.Entries) > 0{
									rf.mu.Lock()
									rf.nextIndex[i] = args.Entries[len(args.Entries) -1].Index + 1
									//fmt.Printf("nextindex %d:%d\n",i,rf.nextIndex[i])
									rf.matchIndex[i] = rf.nextIndex[i] - 1
									rf.mu.Unlock()
									//rf.persist()
								}
							}else{
								rf.nextIndex[i] = reply.NextIndex
								//rf.persist()
							}
						}

					}(i, args)
				}
			}

		case <- rf.leaderConvertToFollower:
			rf.state = 0
			rf.votedFor = -1
			rf.persist()
			return
		}
	}

}

func (rf *Raft) loopFunc(){

	for {
		switch state := rf.state; state {
		case 0: //follower
			rf.followerHandle()
			break
		case 1://candidate
			rf.candidateHandle()
			break
		case 2: //leader
			rf.leaderHandle()
			break
		default:
			fmt.Println("func: loopFunc : The state is wrong")
			break
		}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	isLeader := false
	if rf.state != 2{
		return index, term, isLeader
	}

	isLeader = true
	index = rf.log[len(rf.log)-1].Index + 1
	//fmt.Println("Start\n")
	logEntry := Entry{
		index,
		term,
		command,
	}
	rf.log = append(rf.log, logEntry)
	rf.persist()

	// send the server heart beart channel to append log to other servers
	rf.heartBeatChan <- 1


	//fmt.Printf("me:%d %v, index:%d\n",rf.me,command, index)

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

	// initialize the state
	rf.state = 0


	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}


	rf.electionTime = time.NewTimer(2*MinimumElectionTimeoutMS*time.Millisecond)

	rf.candiConvertToFollower = make(chan int)
	rf.leaderConvertToFollower = make(chan int)

	rf.heartBeatChan = make(chan int)
	rf.commitChan = make(chan int)

	rf.resetElectionTimeout()
	rf.log = append(rf.log, Entry{Term: 0})
	rf.readPersist(persister.ReadRaftState())

	//fmt.Printf("server %d: log %v\n", rf.me, rf.log)

	// end initialize the state

	go func(){
		for{
			select{
			case <- rf.commitChan:
				commitIndex := rf.commitIndex
				//baseIndex := rf.log[0].Index
				for i := rf.lastApplied + 1; i <= commitIndex && i < len(rf.log); i++{
					msg := ApplyMsg{Index : i, Command : rf.log[i].Command}
					applyCh <- msg
					//fmt.Printf("me:%d %v %d\n", rf.me, msg, rf.state)
					rf.lastApplied = i

				}
			}
		}
	}()
	go rf.loopFunc()



	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())



	return rf
}
