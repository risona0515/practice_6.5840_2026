package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type (
	TTermIndex uint32
	TLogIndex  uint64
	// TPeerID    uint32
	TState int
)

type Log struct {
	Term  TTermIndex
	Index TLogIndex
	Data  any
}

const (
	IDLE TState = iota
	SYNCING
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	curLeader   int
	peersCnt    int
	leaderAlive bool // 起到定时器的作用。也可以通过记录时间来达到目的

	// Persistent
	currentTerm   TTermIndex
	votedFor      int
	logs          []Log
	firstLogIndex TLogIndex

	// Volatile
	individualLocks []sync.Mutex
	peerStates      []TState

	commitIndex TLogIndex
	lastApplied TLogIndex

	prevIndexes  []TLogIndex
	nextIndexes  []TLogIndex
	matchIndexes []TLogIndex

	appChan chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).
	term = int(rf.currentTerm)
	isleader = rf.curLeader == rf.me
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
	// Your code here (3C).
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
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// AppendEntries <- broadcast <- handler

type AppendEntriesArgs struct {
	Term            TTermIndex
	LeaderID        int
	PrevLogIndex    TLogIndex
	PrevLogTerm     TTermIndex
	Entries         []Log
	LeaderCommitIdx TLogIndex
}

type AppendEntriesReply struct {
	Term    TTermIndex
	Success bool

	ConflictTerm           TTermIndex
	ConflictTermFirstIndex TLogIndex
	LastTerm               TTermIndex
	LastIndex              TLogIndex
}

func (rf *Raft) isConflict(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	// 如果append entries中第一个日志比最新的日志还大
	// 返回失败，填写自己最新的日志的位置，等待leader重新通知
	latestLog := rf.logs[len(rf.logs)-1]
	if args.PrevLogIndex > latestLog.Index {
		// reply.Success = false
		reply.LastIndex = latestLog.Index
		reply.Term = latestLog.Term
		return true
	}

	prevLog := rf.logs[args.PrevLogIndex-rf.firstLogIndex]
	if prevLog.Term != args.PrevLogTerm {
		// reply.Success = false
		reply.ConflictTerm = prevLog.Term
		reply.ConflictTermFirstIndex = rf.commitIndex
		for idx := prevLog.Index; idx >= rf.firstLogIndex; idx-- {
			if rf.logs[idx-rf.firstLogIndex].Term != prevLog.Term {
				reply.ConflictTermFirstIndex = idx + 1
				break
			}
		}
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// if args.Term > rf.currentTerm {
	// rf.curLeader = args.LeaderID
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	// return
	// }
	// 有没有可能同term但多个server发来了append entries？
	// if args.LeaderID != rf.curLeader {
	// 	reply.Success = false
	// }
	// 如果一个新的同term的append entries与自己的leader不一样，更新？
	rf.leaderAlive = true
	rf.curLeader = args.LeaderID
	rf.persist()
	if args.Entries == nil { // keep alive heartbeat
		// rf.leaderAlive = true
		if rf.isConflict(args, reply) {
			reply.Success = false
		} else {
			reply.Success = true
		}
		return
	}

	// 如果append entries中最新的日志都比已经commit的小
	// 感觉这个分支走不进来
	if rf.commitIndex > args.Entries[len(args.Entries)-1].Index {
		reply.Success = true
		return
	}

	if rf.isConflict(args, reply) {
		reply.Success = false
		return
	}

	// 拼上去

	// 感觉不太可能commit还大于第一个？
	// startPos := 0
	// if rf.commitIndex >= args.Entries[startPos].index {
	// 	startPos = int(rf.commitIndex - args.Entries[startPos].index + 1)
	// }
	// startIndex := args.Entries[startPos].index
	// rf.logs = append(rf.logs[:(startIndex-rf.firstLogIndex)], args.Entries[startPos:]...)

	rf.logs = append(rf.logs[:args.Entries[0].Index], args.Entries...)

	// 然后commit
	rf.commitIndex = min(args.LeaderCommitIdx, rf.logs[len(rf.logs)-1].Index)

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) keepalive() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.curLeader != rf.me {
		return
	}

	args := AppendEntriesArgs{
		Term:            rf.currentTerm,
		LeaderID:        rf.me,
		PrevLogIndex:    rf.logs[len(rf.logs)-1].Index,
		PrevLogTerm:     rf.logs[len(rf.logs)-1].Term,
		Entries:         nil,
		LeaderCommitIdx: 0,
	}

	replies := make([]AppendEntriesReply, rf.peersCnt)
	var hasNewLeader atomic.Bool
	hasNewLeader.Store(false) // 记录是否有新的leader

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	for i := 0; i < rf.peersCnt; i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch := make(chan bool, 1)
			go func() {
				ok := rf.sendAppendEntries(i, &args, &(replies[i]))
				ch <- ok
			}()
			select {
			case ok := <-ch:
				if !ok {
					// log.Printf("leader %v send appendentries to %v failed", rf.me, i)
				}
				if !hasNewLeader.Load() && replies[i].Term > rf.currentTerm {
					hasNewLeader.Store(true)
					rf.curLeader = i
					rf.currentTerm = replies[i].Term
					rf.persist()
					return
				}
				if !replies[i].Success {
					rf.getCommonPosAndRetry(i, &replies[i])
				}
				return
			case <-ctx.Done():
				return
			}
		}(i)
		if hasNewLeader.Load() {
			break
		}
	}
}

// lock outside of this function
// func (rf *Raft) broadcaster(args []AppendEntriesArgs, replies []AppendEntriesReply) {
// 	var wg sync.WaitGroup
// 	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
// 	defer cancel()

// 	var hasNewLeader atomic.Bool
// 	hasNewLeader.Store(false) // 记录是否有新的leader

// 	for i := 0; i < rf.peersCnt; i++ {
// 		if i == rf.me {
// 			continue
// 		}
// 		wg.Add(1)
// 		go func(i int) {
// 			defer wg.Done()
// 			ch := make(chan bool, 1)
// 			go func() {
// 				ok := rf.sendAppendEntries(i, &(args[i]), &(replies[i]))
// 				ch <- ok
// 			}()
// 			select {
// 			case ok := <-ch:
// 				if !ok {
// 					// log.Printf("leader %v send appendentries to %v failed", rf.me, i)
// 					return
// 				}
// 				if !hasNewLeader.Load() && replies[i].Term > rf.currentTerm {
// 					hasNewLeader.Store(true)
// 					rf.curLeader = i
// 					rf.currentTerm = replies[i].Term
// 					rf.persist()
// 					return
// 				}
// 				return
// 			case <-ctx.Done():
// 				return
// 			}
// 		}(i)
// 		if hasNewLeader.Load() {
// 			break
// 		}
// 	}
// }

// func (rf *Raft) broadcast(args *AppendEntriesArgs) {

// 	replies := make([]AppendEntriesReply, rf.peersCnt)

// 	for i := 0; i < rf.peersCnt; i++ {
// 		if i == rf.me {
// 			continue
// 		}
// 		go func() {
// 			ok := rf.sendAppendEntries(i, args, &(replies[i]))
// 		}()

// 	}
// }

// func broadcaster[T1 any, T2 any](rf *Raft, f func(int, *T1, *T2) bool, args []T1, replies []T2) {
// 	var wg sync.WaitGroup
// 	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
// 	defer cancel()

// 	var hasNewLeader atomic.Bool
// 	hasNewLeader.Store(false) // 记录是否有新的leader

// 	for i := 0; i < rf.peersCnt; i++ {
// 		if i == rf.me {
// 			continue
// 		}
// 		wg.Add(1)
// 		go func(i int) {
// 			defer wg.Done()
// 			ch := make(chan bool, 1)
// 			go func() {
// 				ok := f(i, &(args[i]), &(replies[i]))
// 				ch <- ok
// 			}()
// 			select {
// 			case ok := <-ch:
// 				if !ok {
// 					return
// 				}

// 				return
// 			case <-ctx.Done():
// 				return
// 			}
// 		}(i)
// 		if hasNewLeader {
// 			break
// 		}
// 	}
// }

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         TTermIndex
	CandidateId  int
	LastLogIndex TLogIndex
	LastLogTerm  TTermIndex
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        TTermIndex
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// log.Printf("%v receive request vote rpc from %v", rf.me, args.CandidateId)
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// log.Printf("%v reject %v request vote, args term %v, cur term %v", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.curLeader = -1
	}

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	lastLog := rf.logs[len(rf.logs)-1]
	upToData := args.LastLogTerm > lastLog.Term ||
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)

	if canVote && upToData {
		rf.votedFor = args.CandidateId
		rf.curLeader = args.CandidateId
		reply.VoteGranted = true
		rf.leaderAlive = true
		log.Printf("%v grant %v request vote, voted for %v, term %v", rf.me, args.CandidateId, rf.votedFor, rf.currentTerm)
	} else {
		reply.VoteGranted = false
	}
	rf.persist()
	// if args.Term == rf.currentTerm {
	// 	if args.CandidateId == rf.votedFor || rf.votedFor == math.MaxInt32 {
	// 		rf.votedFor = args.CandidateId
	// 		reply.VoteGranted = true
	// 		log.Printf("%v grant %v request vote, voted for %v, term %v", rf.me, args.CandidateId, rf.votedFor, rf.currentTerm)
	// 	} else {
	// 		reply.VoteGranted = false
	// 		log.Printf("%v reject %v request vote, voted for %v, term %v", rf.me, args.CandidateId, rf.votedFor, rf.currentTerm)
	// 	}
	// 	return
	// }

	// rf.currentTerm = args.Term
	// rf.votedFor = math.MaxInt32

	// latestlog := rf.logs[len(rf.logs)-1]
	// if latestlog.Index <= args.LastLogIndex && latestlog.Term <= args.LastLogTerm {
	// 	log.Printf("%v grant %v request vote, args term %v, cur term %v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	// 	log.Printf("args logid %v logterm %v, me logid %v, logterm %v\n", args.LastLogIndex, args.LastLogTerm, latestlog.Index, latestlog.Term)

	// 	rf.votedFor = args.CandidateId
	// 	// rf.currentTerm = args.Term
	// 	rf.curLeader = args.CandidateId
	// 	reply.VoteGranted = true
	// 	reply.Term = rf.currentTerm
	// } else {

	// 	log.Printf("%v rrreject %v request vote, args term %v, cur term %v\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	// 	log.Printf("args logid %v logterm %v, me logid %v, logterm %v\n", args.LastLogIndex, args.LastLogTerm, latestlog.Index, latestlog.Term)
	// 	reply.VoteGranted = false
	// }
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
	// log.Printf("%v send request vote to %v, %v", rf.me, server, ok)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.curLeader == rf.me
	// log.Printf("me %v isleader %v curleader %v", rf.me, isLeader, rf.curLeader)
	if !isLeader {
		return -1, -1, isLeader
	}

	// Your code here (3B).

	term := rf.currentTerm

	lastlog := rf.logs[len(rf.logs)-1]
	index := lastlog.Index + 1
	newlog := Log{rf.currentTerm, index, command}
	rf.logs = append(rf.logs, newlog)
	go rf.broadcastNewLog(index)

	log.Printf("*******Start function print all logs*******")
	for idx := 0; idx < len(rf.logs); idx++ {
		log.Printf("me %v, leader %v, log %v %v", rf.me, rf.curLeader, idx, rf.logs[idx])
	}
	log.Printf("*******print end******")

	return int(index), int(term), isLeader
}

func (rf *Raft) broadcastNewLog(index TLogIndex) {
	_ = index // unreferenced param
	for i := 0; i < rf.peersCnt; i++ {
		if i == rf.me {
			continue
		}
		go rf.appendNewLog(i)
	}
}

func (rf *Raft) waitAndRetryAppendNewLog(server int) {
	time.Sleep(200 * time.Millisecond)
	rf.appendNewLog(server)
}

func (rf *Raft) getCommonPosAndRetry(server int, reply *AppendEntriesReply) {
	rf.individualLocks[server].Lock()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.individualLocks[server].Unlock()

	prevLogIdx := rf.prevIndexes[server]
	prevLog := rf.logs[prevLogIdx-rf.firstLogIndex]

	if reply.LastIndex < prevLog.Index {
		if reply.LastIndex < rf.firstLogIndex {
			// install snapshot
			// append
			return
		} else {
			// followerLastLog := rf.logs[reply.LastIndex-rf.firstLogIndex]
			rf.prevIndexes[server] = reply.LastIndex
			rf.appendNewLog(server)
			return
		}
	}

	conflictTermFirstLog := rf.logs[reply.ConflictTermFirstIndex-rf.firstLogIndex]
	if conflictTermFirstLog.Term == reply.ConflictTerm {
		rf.prevIndexes[server] = reply.ConflictTermFirstIndex
	} else {
		rf.prevIndexes[server] = reply.ConflictTermFirstIndex - 1
	}
	rf.appendNewLog(server)

}

func (rf *Raft) appendNewLog(server int) {
	rf.individualLocks[server].Lock()
	defer rf.individualLocks[server].Unlock()

	if rf.peerStates[server] == SYNCING {
		return
	}
	rf.peerStates[server] = SYNCING

	prevLogIdx := rf.prevIndexes[server]
	prevLog := rf.logs[prevLogIdx]
	lastLog := rf.logs[len(rf.logs)-1]

	args := AppendEntriesArgs{
		Term:     lastLog.Term,
		LeaderID: rf.me,
	}
	args.PrevLogIndex = prevLog.Index
	args.PrevLogTerm = prevLog.Term
	args.Entries = rf.logs[prevLogIdx+1:]
	args.LeaderCommitIdx = rf.commitIndex

	reply := AppendEntriesReply{}

	ok := rf.sendAppendEntries(server, &args, &reply)

	if !ok {
		rf.waitAndRetryAppendNewLog(server)
	}
	rf.peerStates[server] = IDLE
	if !reply.Success {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.curLeader = server
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.getCommonPosAndRetry(server, &reply)
		return
	}

	rf.prevIndexes[server] = lastLog.Index
}

func (rf *Raft) checkTimeoutAndVoteSelf() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leaderAlive || rf.curLeader == rf.me {
		rf.leaderAlive = false
		return
	}

	rf.currentTerm++ // 直接++，还是等成为leader再++？
	rf.votedFor = rf.me
	rf.persist()
	latestlog := rf.logs[len(rf.logs)-1]
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: latestlog.Index,
		LastLogTerm:  latestlog.Term,
	}
	replies := make([]RequestVoteReply, rf.peersCnt)

	// log.Printf("%v timeout request vote term %v", rf.me, rf.currentTerm+1)

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var count atomic.Int32 // 统计同意的人数
	var hasNewLeader atomic.Bool
	hasNewLeader.Store(false) // 记录是否有新的leader

	if rf.me == 0 {
		// log.Printf("aaa %v", rf.peersCnt)
	}
	for i := 0; i < rf.peersCnt; i++ {

		if rf.me == 0 {
			// log.Printf("bbb")
		}
		if i == rf.me {
			count.Add(1)
			continue
		}
		wg.Add(1)

		if rf.me == 0 {
			// log.Printf("ccc")
		}
		go func(i int) {

			if rf.me == 0 {
				// log.Printf("ddd")
			}
			defer wg.Done()
			ch := make(chan bool, 1)
			go func() {

				// log.Printf("%v start go routine send request vote to %v", rf.me, i)
				ok := rf.sendRequestVote(i, &args, &(replies[i]))
				ch <- ok
			}()
			// ok := rf.peers[i].Call("Raft.RequestVote", &args, &(replies[i]))
			select {
			case ok := <-ch:
				if !ok {
					return
				}
				if !hasNewLeader.Load() && replies[i].Term > rf.currentTerm {
					hasNewLeader.Store(true)
					rf.curLeader = i
					rf.currentTerm = replies[i].Term
					rf.persist()
					return
				}
				if replies[i].VoteGranted {
					count.Add(1)
				}
			case <-ctx.Done():
				return
			}
		}(i)
		if hasNewLeader.Load() { // 这个不一定要跟前面赋值的地方一一对应。只要在循环创建goroutine过程中发现了新leader就直接退出
			break
		}
	}
	wg.Wait()
	if !hasNewLeader.Load() && int(count.Load()) > rf.peersCnt/2 {
		rf.curLeader = rf.me // become leader
		rf.persist()
		// rf.currentTerm++
		latestIndex := rf.logs[len(rf.logs)-1].Index
		for idx := 0; idx < rf.peersCnt; idx++ {
			if idx == rf.me {
				continue
			}
			rf.prevIndexes[idx] = latestIndex
		}
		log.Printf("%v leader granted, cur term %v", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.checkTimeoutAndVoteSelf()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)

		// 1.5s - 2s. lab requested no more than 10 rpc per second, and vote a new leader within 5s
		// heartbeat gap set to 150ms
		ms := 500 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// periodly send heartbeat
func (rf *Raft) leaderTicker() {
	for true {
		rf.keepalive()

		// 1.5s - 2s. lab requested no more than 10 rpc per second, and vote a new leader within 5s
		// heartbeat gap set to 150ms
		time.Sleep(100 * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.peersCnt = len(rf.peers)
	rf.leaderAlive = true // 初始化为true，等定时器自己超时
	rf.curLeader = -1
	rf.logs = make([]Log, 1) // 初始化一个全0的初始log方便RequestVote里统一逻辑
	rf.individualLocks = make([]sync.Mutex, rf.peersCnt)
	rf.peerStates = make([]TState, rf.peersCnt)
	rf.prevIndexes = make([]TLogIndex, rf.peersCnt)
	rf.nextIndexes = make([]TLogIndex, rf.peersCnt)
	rf.matchIndexes = make([]TLogIndex, rf.peersCnt)
	rf.appChan = applyCh

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.leaderTicker()

	return rf
}
