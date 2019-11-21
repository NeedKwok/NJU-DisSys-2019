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

	state   int //1:leader ; 2:follower ; 3:candidate
	applyCh chan ApplyMsg

	timer      *time.Timer
	votesCount int
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) RestartTime() {

	randTime := ElectionMinTime + rand.Int63n(ElectionRandTime)
	timeout := time.Millisecond * time.Duration(randTime)
	if rf.state == 1 {
		timeout = HeartbeatTime * time.Millisecond
		randTime = HeartbeatTime
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

func (rf *Raft) TimeOutFunc() { //超时了做什么，如果不是leader，都会发起竞选，如果是leader,实际上只是计时器到了发送心跳

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.RestartTime() //重置超时计时器

	if rf.state != 1 { //不是leader,则发起竞选
		rf.state = 3
		rf.currentTerm += 1
		rf.votedFor = rf.me
		rf.votesCount = 1 //先给自己投票
		rf.persist()

		var args RequestVoteArgs
		args.Term = rf.currentTerm
		args.CandidateId = rf.me

		for peer := 0; peer < len(rf.peers); peer++ { // 给每个几点发送请求，拉票
			if peer == rf.me {
				continue
			}

			go func(peer int, args RequestVoteArgs) {
				var reply RequestVoteReply
				ok := rf.peers[peer].Call("Raft.RequestVote", args, &reply)
				if ok {
					rf.CountVote(reply)
				}
			}(peer, args)

		}
	} else { //如果是leader,则发送心跳
		rf.SendAppendEntries()
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

	} else if rf.state == 3 && reply.VoteGranted {
		rf.votesCount += 1
		if rf.votesCount >= (len(rf.peers)+2)/2 {
			rf.state = 1 //选举成功
			/*
				这里leader 应该做些什么，但是在本实验中没必要
			*/
			rf.RestartTime()
			rf.SendAppendEntries() //需要立即发送心跳稳固自己的地位
		}
		return
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

/*func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}*/

//Invoked by leader to replicate log entries (§5.3);
// also used as heartbeat (§5.2).
func (rf *Raft) SendAppendEntries() {
	// Your code here.
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		var args AppendEntryArgs
		//只需要以下两个变量，其他在下次实验可能用到
		args.Term = rf.currentTerm
		args.LeaderId = rf.me

		go func(server int, args AppendEntryArgs) { //发送心跳
			var reply AppendEntryReply
			ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
			if ok {
				rf.HandleAppendEntries(server, reply)
			}
		}(i, args)
	}

}

func (rf *Raft) HandleAppendEntries(server int, reply AppendEntryReply) { //处理心跳的返回
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.RestartTime()
	if rf.state != 1 {
		return
	} else {
		if reply.Term > rf.currentTerm { //自己的任期不是最大
			rf.state = 2
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return
		}
	}
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

func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.RestartTime()
	if rf.state == 1 { //如果是leader，什么都不做（理论上leader不会执行这个函数）
		return
	} else { // 如果不是leader
		if args.Term < rf.currentTerm { //第一条规则
			reply.Success = false
			reply.Term = rf.currentTerm
		} else {
			reply.Term = args.Term

			rf.state = 2
			rf.currentTerm = args.Term
			rf.votedFor = -1
			if args.PrevLogIndex >= 0 && (len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) { //第二条规则
				reply.Success = false //由于有关日志同步的值全部为默认值，所以这个reply一定为false，但不影响选举
			} else {

				reply.Success = true
			}

			rf.persist()
		}
	}
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
//来自图2 RequestVote   RPC Arguments:
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here.
}

//
// example RequestVote RPC reply structure.
//
//来自图2 RequestVote   RPC Results:
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here.
}

//
// example RequestVote RPC handler.
//
//来自图2 RequestVote RPC   Receiver implementation:
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	length := len(rf.log)
	hasVoted := rf.votedFor != -1
	hasNewerLog := length > 0 && (length > args.LastLogIndex || rf.log[length-1].Term > args.LastLogTerm)

	//1. Reply false if term < currentTerm (§5.1)
	//2. If votedFor is null or candidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	rf.mu.Lock()         // 加锁
	defer rf.mu.Unlock() //当函数执行结束，解锁

	if args.Term < rf.currentTerm { //其他节点的任期小，直接拒绝，并提示其他节点修改任期
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm { //任期相同
		if !hasVoted && !hasNewerLog { //如果没投过票且log没有candidate新
			reply.Term = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.state = 2
			rf.persist()
			rf.RestartTime()
		} else {
			reply.Term = args.Term
			reply.VoteGranted = false
		}
	} else { //其他节点任期大，给他投票，并且置自己为follower，同步任期,持久化
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.state = 2
		rf.persist()
		rf.RestartTime()
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
	isLeader := false

	if rf.state != 1 {
		return index, term, isLeader
	}

	var newLog LogEntry
	newLog.Command = command
	newLog.Term = rf.currentTerm
	rf.log = append(rf.log, newLog)
	index = len(rf.log)
	isLeader = true
	term = rf.currentTerm
	rf.persist()

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

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0) //size = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers)) //还没搞懂这里是什么意思，但是不影响
	rf.matchIndex = make([]int, len(peers))

	rf.state = 2 //follower
	rf.applyCh = applyCh
	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.RestartTime()

	return rf
}
