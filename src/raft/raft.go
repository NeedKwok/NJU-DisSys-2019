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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
const ( //常量
	HeartbeatTime    = 100 //心跳间隔
	ElectionMinTime  = 150
	ElectionRandTime = 150
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct { //根据图2 一个日志条目应该具有的内容
	Command interface{} //状态机命令
	Term    int         //leader接到项时的任期
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct { //服务器结构
	mu        sync.Mutex //锁
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	currentTerm int        //当前任期
	votedFor    int        //为谁投票
	log         []LogEntry //日志条目

	commitIndex int //被提交的最高日志项 单增
	lastApplied int //应用于状态机的最高日志项 单增
	//下面为可变
	nextIndex  []int //要发往这台服务器的下一个日志项索引，初始化为leader的最后一条日志的索引+1
	matchIndex []int //要在服务器上复制的最高日志项的索引，初始化0，单增

	state   int //1:leader ; 2:follower ; 3:candidate ; 4:precandidate
	applyCh chan ApplyMsg

	timer         *time.Timer
	votesCount    int
	preVotesCount int //预选举票数
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) RestartTime() {
	randTime := ElectionMinTime + rand.Int63n(ElectionRandTime)
	timeout := time.Millisecond * time.Duration(randTime)
	if rf.state == 1 {
		timeout = HeartbeatTime * time.Millisecond
		//randTime = HeartbeatTime
	}
	if rf.timer == nil {
		rf.timer = time.NewTimer(timeout)
		go func() {
			for {
				<-rf.timer.C

				rf.TimeOutFunc()
			}
		}()
	}
	rf.timer.Reset(timeout)
}

// example RequestVote RPC arguments structure.
//来自图2 RequestVote   RPC Arguments:
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here.
}

// example RequestVote RPC reply structure.
//来自图2 RequestVote   RPC Results:
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here.
}

type PreVoteReply struct {
	Term int
}
type PreVoteArgs struct {
	Term int
}

func (rf *Raft) ApplyCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("%d,%d,server = %d\n", rf.commitIndex, rf.lastApplied,rf.me)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		var args ApplyMsg
		args.Index = i
		args.Command = rf.log[i].Command
		rf.applyCh <- args
	}
	rf.lastApplied = rf.commitIndex
	//fmt.Printf("%d,%d,server = %d\n", rf.commitIndex, rf.lastApplied,rf.me)
}

func (rf *Raft) TimeOutFunc() { //超时了做什么，如果不是leader，都会发起竞选，如果是leader,实际上只是计时器到了发送心跳

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.RestartTime() //重置超时计时器

	if rf.state != 1 { //不是leader,则发起竞选
		if rf.state == 3 {
			//rf.state = 3
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.votesCount = 1 //先给自己投票
			rf.persist()
			////fmt.Printf("%d term = %d start ele\n",rf.me,rf.currentTerm)
			var args RequestVoteArgs
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[args.LastLogIndex].Term

			for peer := 0; peer < len(rf.peers); peer++ { // 给每个几点发送请求，拉票
				if peer == rf.me {
					continue
				}
				go func(peer int, args RequestVoteArgs) {
					var reply RequestVoteReply
					ok := rf.peers[peer].Call("Raft.ReplyRequestVote", args, &reply)
					if ok {
						rf.CountVote(reply)
					}
				}(peer, args)

			}
		} else { //预选举
			//fmt.Printf("预选举\n")
			rf.state = 4
			rf.preVotesCount = 1
			var args PreVoteArgs
			args.Term = rf.currentTerm
			for peer := 0; peer < len(rf.peers); peer++ { // 给每个几点发送请求，拉票
				if peer == rf.me {
					continue
				}
				go func(peer int, args PreVoteArgs) {
					var reply PreVoteReply
					ok := rf.peers[peer].Call("Raft.ReplyPreVote", args, &reply)
					if ok {
						rf.CountPreVote(reply)
					}
				}(peer, args)

			}
		}

	} else { //如果是leader,则发送心跳
		rf.SendAppendEntries()
	}
}

func (rf *Raft) CountPreVote(reply PreVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm { //如果返回的term更大，说明自己过时了
		rf.currentTerm = reply.Term
		rf.state = 2
		rf.votedFor = -1
		rf.persist()
	}
	if rf.state == 4 {
		rf.preVotesCount += 1
		if rf.preVotesCount >= (len(rf.peers)+2)/2 {
			rf.state = 3 //正式开始选举
			//fmt.Printf("YYYYY\n")
			//rf.TimeOutFunc()
			//rf.RestartTime()
		}
	}
}

func (rf *Raft) CountVote(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//rf.RestartTime()
	if reply.Term > rf.currentTerm { //如果返回的term更大，说明自己过时了
		rf.currentTerm = reply.Term
		rf.state = 2
		rf.votedFor = -1
		rf.persist()
	} else if rf.state == 3 && reply.VoteGranted {
		rf.votesCount += 1
		if rf.votesCount >= (len(rf.peers)+2)/2 {
			rf.state = 1 //选举成功
			//准备发送appendentries
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			rf.RestartTime()
			rf.SendAppendEntries() //需要立即发送心跳稳固自己的地位
		}
	}
}

func (rf *Raft) ReplyPreVote(args PreVoteArgs, reply *PreVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	return
}

// example RequestVote RPC handler.
//来自图2 RequestVote RPC   Receiver implementation:
func (rf *Raft) ReplyRequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	length := len(rf.log)
	iHadVoted := rf.votedFor != -1 && rf.votedFor != args.CandidateId
	iHaveNewerLog := /*length-1 > args.LastLogIndex ||*/ rf.log[length-1].Term > args.LastLogTerm || (length-1 > args.LastLogIndex && rf.log[length-1].Term == args.LastLogTerm)
	//if !iHadVoted {
	//	fmt.Printf("XXXX\n")
	//}
	//if !iHaveNewerLog {
	//	fmt.Printf("YYYY\n")
	//}
	rf.mu.Lock()         // 加锁
	defer rf.mu.Unlock() //当函数执行结束，解锁

	if args.Term < rf.currentTerm { //其他节点的任期小，直接拒绝，并提示其他节点修改任期
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		////fmt.Printf("%d term = %d,but %d term is %d\n",args.CandidateId,args.Term,rf.me,rf.currentTerm)
	} else if args.Term == rf.currentTerm { //任期相同
		if !iHadVoted && !iHaveNewerLog { //如果没投过票且log没有candidate新
			reply.Term = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.state = 2
			rf.persist()
			rf.RestartTime()
		} else {
			reply.Term = args.Term
			reply.VoteGranted = false
			//rf.RestartTime()
		}
	} else { //其他节点任期大，置自己为follower，同步任期,持久化,但不一定给他投票
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.state = 2
		rf.votedFor = -1
		if !iHaveNewerLog { //如果log没有candidate新
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.RestartTime()
		} else {
			reply.VoteGranted = false
		}
		rf.persist()
	}
}

//AppendEntries
type AppendEntryArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s LeaderCommit
}

type AppendEntryReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

//Invoked by leader to replicate log entries (§5.3);
// also used as heartbeat (§5.2).
func (rf *Raft) SendAppendEntries() {
	// Your code here.
	//fmt.Printf("SendAppendEntries:%d,%d\n",rf.commitIndex,rf.lastApplied)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var args AppendEntryArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		if rf.nextIndex[i] <= len(rf.log)-1 {
			args.Entries = rf.log[rf.nextIndex[i]:]
		} //else args.Entries = nil
		args.LeaderCommit = rf.commitIndex

		go func(server int, args AppendEntryArgs) { //发送心跳
			var reply AppendEntryReply
			ok := rf.peers[server].Call("Raft.ReplyAppendEntries", args, &reply)
			if ok {
				rf.HandleAppendEntries(server, reply)
			}
		}(i, args)
	}

}

func (rf *Raft) HandleAppendEntries(server int, reply AppendEntryReply) { //处理AppendEntries的返回
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//rf.RestartTime()
	if rf.state != 1 {
		return
	} else {
		if reply.Term > rf.currentTerm { //自己的任期不是最大
			rf.state = 2
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			rf.RestartTime()
			return
		} else {
			if reply.Success { // If successful: update nextIndex and matchIndex for follower (§5.3)
				rf.nextIndex[server] = len(rf.log)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else { //If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
				rf.nextIndex[server]--
			}
			N := rf.SearchN()
			if N > rf.commitIndex { //对应leader操作的最后一条
				rf.commitIndex = N
				go rf.ApplyCommit()
			}
		}
	}
}

func (rf *Raft) SearchN() int { //对应leader操作的最后一条
	N := rf.commitIndex + 1
	count := 1
	length := len(rf.peers)
	for i := 0; i < length; i++ {
		////fmt.Printf("rf.matchIndex[%d] = %d\n",i, rf.matchIndex[i])
		if rf.me == i {
			continue
		}
		if rf.matchIndex[i] >= N {
			count++
		}
		if count > length/2 {
			N++
			i = 0
			count = 1
		}
	}
	////fmt.Printf("N - 1 = %d\n",N - 1)
	return N - 1
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex
//whose term matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index
//but different terms), delete the existing entry and all that
//follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex =
//min(leaderCommit, index of last new entry)

func (rf *Raft) ReplyAppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.RestartTime()

	if args.Term < rf.currentTerm { //第一条规则
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Term = args.Term
		rf.state = 2
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if args.PrevLogIndex > len(rf.log)-1 || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term { //第二条规则
			reply.Success = false
		} else {
			reply.Success = true
			if args.Entries != nil {
				rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...) //规则3，4
			}
			if args.LeaderCommit > rf.commitIndex { //规则 5
				if args.LeaderCommit <= len(rf.log)-1 {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = len(rf.log) - 1
				}

				if rf.commitIndex > rf.lastApplied {
					go rf.ApplyCommit()
				}
			}
		}
		rf.persist()
	}
	////fmt.Printf("i am follower-%d,my log len = %d\n",rf.me, len(rf.log))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { //获取服务器的任期和他是否认为自己是leader

	term := rf.currentTerm
	isLeader := rf.state == 1 //1 is leader
	// Your code here.
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() { //
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	//
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

//func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

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
	index := -1
	term := -1

	if rf.state != 1 {
		return index, term, false
	}

	var newLog LogEntry
	newLog.Command = command
	newLog.Term = rf.currentTerm
	rf.log = append(rf.log, newLog)
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.persist()
	////fmt.Printf("index = %d,term = %d,rf.me = %d,commitIndex = %d\n", index, term,rf.me,rf.commitIndex)
	////fmt.Printf("i am leader-%d,my log len = %d\n",rf.me, len(rf.log))
	return index, term, true
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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) //初始大小为1，因为开始大家都是follower，log[0]中存放默认值

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = 2 //follower
	rf.applyCh = applyCh
	// Your initialization code here.
	// initialize from state persisted before a crash
	rf.readPersist(rf.persister.ReadRaftState())
	rf.persist()
	rf.RestartTime()

	return rf
}
