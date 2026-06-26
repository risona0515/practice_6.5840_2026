package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"context"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type (
	TTermIndex uint32
	TLogIndex  uint64
	TState     int
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

	curLeader   int
	peersCnt    int
	leaderAlive bool // acts as a timer: true means we've heard from a leader recently

	// Persistent state (Figure 2)
	currentTerm TTermIndex
	votedFor    int
	logs        []Log // logs[0] is a sentinel at firstLogIndex; real entries follow

	// Volatile state (Figure 2)
	commitIndex TLogIndex
	lastApplied TLogIndex

	// Leader state (Figure 2), reinitialized after election
	nextIndexes  []TLogIndex
	matchIndexes []TLogIndex

	// Snapshot state (3D)
	snapshot []byte // cached snapshot bytes

	// Channel to apply committed entries
	appChan chan raftapi.ApplyMsg

	// Channel to trigger immediate replication
	replicateCh chan struct{}

	// For checking if server is dead
	dead int32
}

// --- Log helpers ---

// firstLogIndex returns the logical index of the first entry in logs (the sentinel).
func (rf *Raft) firstLogIndex() TLogIndex {
	return rf.logs[0].Index
}

// lastLogIndex returns the logical index of the last entry in logs.
func (rf *Raft) lastLogIndex() TLogIndex {
	return rf.logs[len(rf.logs)-1].Index
}

// lastLogTerm returns the term of the last entry in logs.
func (rf *Raft) lastLogTerm() TTermIndex {
	return rf.logs[len(rf.logs)-1].Term
}

// logAt returns the log entry at the given logical index.
// Panics if idx < firstLogIndex or idx > lastLogIndex.
func (rf *Raft) logAt(idx TLogIndex) Log {
	return rf.logs[idx-rf.firstLogIndex()]
}

// --- Raft API ---

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := int(rf.currentTerm)
	isleader := rf.curLeader == rf.me
	return term, isleader
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// --- Persistence (3C) ---

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// persist saves Raft's persistent state to stable storage.
// Must be called with rf.mu held.
func (rf *Raft) persist() {
	raftstate := rf.encodeRaftState()
	rf.persister.Save(raftstate, rf.snapshot)
}

// readPersist restores previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm TTermIndex
	var votedFor int
	var logs []Log

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		log.Printf("readPersist: decode error")
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs

	// After restore, ensure commitIndex and lastApplied are at least firstLogIndex
	firstIdx := rf.firstLogIndex()
	if rf.commitIndex < firstIdx {
		rf.commitIndex = firstIdx
	}
	if rf.lastApplied < firstIdx {
		rf.lastApplied = firstIdx
	}
}

// --- Snapshot (3D) ---

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	idx := TLogIndex(index)
	firstIdx := rf.firstLogIndex()

	// If the snapshot is for an index we've already snapshotted (or before), ignore
	if idx <= firstIdx {
		return
	}

	// If the snapshot index is beyond our last log, we can't trim that far yet
	lastIdx := rf.lastLogIndex()
	if idx > lastIdx {
		idx = lastIdx
	}

	// Find the position of the entry at 'idx' in the logs slice
	pos := idx - firstIdx
	if pos >= TLogIndex(len(rf.logs)) {
		return
	}

	// Keep entries from idx onwards (the entry at idx becomes the new sentinel)
	rf.logs = rf.logs[pos:]
	rf.logs[0] = rf.logs[0] // entry at idx is now logs[0]
	rf.snapshot = snapshot

	// Update commitIndex and lastApplied to be at least firstLogIndex
	if rf.commitIndex < idx {
		rf.commitIndex = idx
	}
	if rf.lastApplied < idx {
		rf.lastApplied = idx
	}

	// Persist raft state and snapshot together
	rf.persist()
}

// --- InstallSnapshot RPC (3D) ---

type InstallSnapshotArgs struct {
	Term              TTermIndex
	LeaderID          int
	LastIncludedIndex TLogIndex
	LastIncludedTerm  TTermIndex
	Data              []byte
}

type InstallSnapshotReply struct {
	Term TTermIndex
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// If we get a message from a higher or equal term, step down
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.leaderAlive = true
	rf.curLeader = args.LeaderID

	// If snapshot is not newer than what we have, ignore
	if args.LastIncludedIndex <= rf.firstLogIndex() {
		return
	}

	// Trim log: keep only entries after LastIncludedIndex
	// Find the position of the first entry > args.LastIncludedIndex
	trimPos := -1
	for i := 0; i < len(rf.logs); i++ {
		if rf.logs[i].Index > args.LastIncludedIndex {
			trimPos = i
			break
		}
	}

	var newLogs []Log
	newLogs = append(newLogs, Log{
		Term:  args.LastIncludedTerm,
		Index: args.LastIncludedIndex,
	})

	if trimPos != -1 {
		newLogs = append(newLogs, rf.logs[trimPos:]...)
	}

	rf.logs = newLogs
	rf.snapshot = args.Data

	// Update commit and lastApplied
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
	}

	// Persist raft state + snapshot together
	rf.persister.Save(rf.encodeRaftState(), args.Data)

	// Send snapshot up through applyCh so the server can ingest it
	rf.appChan <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  int(args.LastIncludedTerm),
		SnapshotIndex: int(args.LastIncludedIndex),
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// --- AppendEntries RPC ---

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

	// For fast conflict resolution
	ConflictTerm  TTermIndex
	ConflictIndex TLogIndex
	LastLogIndex  TLogIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	// 2. If RPC request contains higher term, update term and step down (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// Recognize the leader
	rf.leaderAlive = true
	rf.curLeader = args.LeaderID

	// 3. Reply false if log doesn't contain an entry at PrevLogIndex
	//    whose term matches PrevLogTerm (§5.3)
	firstIdx := rf.firstLogIndex()
	lastIdx := rf.lastLogIndex()

	if args.PrevLogIndex < firstIdx {
		// The leader is sending entries before our snapshot.
		// We can't verify consistency. Tell the leader our first log index
		// so it can send a snapshot.
		reply.LastLogIndex = lastIdx
		return
	}

	if args.PrevLogIndex > lastIdx {
		// We don't have this entry yet (leader is ahead of us).
		reply.LastLogIndex = lastIdx
		return
	}

	// We have the entry at PrevLogIndex; check term match
	prevEntry := rf.logAt(args.PrevLogIndex)
	if prevEntry.Term != args.PrevLogTerm {
		// Conflict: find the first index of the conflicting term
		reply.ConflictTerm = prevEntry.Term
		// Scan backwards to find the first entry with this term
		for i := args.PrevLogIndex; i >= firstIdx; i-- {
			if rf.logAt(i).Term != prevEntry.Term {
				reply.ConflictIndex = i + 1
				break
			}
			if i == firstIdx {
				reply.ConflictIndex = firstIdx
			}
		}
		return
	}

	// 4. Append any new entries not already in the log
	//    If an existing entry conflicts with a new one (same index but different terms),
	//    delete the existing entry and all that follow (§5.3)
	if len(args.Entries) > 0 {
		// Find the first entry that is new or conflicting
		insertPos := args.PrevLogIndex + 1
		logPos := insertPos - firstIdx

		// Skip entries we already have (with matching terms)
		for i := 0; i < len(args.Entries); i++ {
			if logPos >= TLogIndex(len(rf.logs)) {
				// All remaining entries are new
				rf.logs = append(rf.logs, args.Entries[i:]...)
				break
			}
			if rf.logs[logPos].Term != args.Entries[i].Term {
				// Conflict: truncate and append from here
				rf.logs = rf.logs[:logPos]
				rf.logs = append(rf.logs, args.Entries[i:]...)
				break
			}
			// Entry matches, move to next
			logPos++
		}
	}

	// 5. If LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of last new entry)
	if args.LeaderCommitIdx > rf.commitIndex {
		rf.commitIndex = args.LeaderCommitIdx
		if rf.commitIndex > rf.lastLogIndex() {
			rf.commitIndex = rf.lastLogIndex()
		}
	}

	reply.Success = true

	// Persist after modifying logs
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// --- RequestVote RPC ---

type RequestVoteArgs struct {
	Term         TTermIndex
	CandidateId  int
	LastLogIndex TLogIndex
	LastLogTerm  TTermIndex
}

type RequestVoteReply struct {
	Term        TTermIndex
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		return
	}

	// 2. If RPC request contains higher term, update term and step down (§5.1)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.curLeader = -1
		rf.persist()
	}

	// Check if we can vote for this candidate
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId

	// Check if candidate's log is at least as up-to-date as ours (§5.4)
	lastLogIdx := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx)

	if canVote && upToDate {
		rf.votedFor = args.CandidateId
		rf.curLeader = args.CandidateId
		reply.VoteGranted = true
		rf.leaderAlive = true
		rf.persist()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// --- Start: client command submission (3B) ---

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.curLeader == rf.me
	if !isLeader {
		return -1, -1, false
	}

	term := rf.currentTerm
	lastIdx := rf.lastLogIndex()
	newIndex := lastIdx + 1

	newLog := Log{
		Term:  term,
		Index: newIndex,
		Data:  command,
	}
	rf.logs = append(rf.logs, newLog)

	rf.persist()

	// Signal replicator to send entries immediately
	go func() {
		select {
		case rf.replicateCh <- struct{}{}:
		default:
		}
	}()

	return int(newIndex), int(term), true
}

// --- Leader Replication ---

// replicateToPeer sends AppendEntries (or InstallSnapshot) to a single peer.
// Called from replicateLoop with rf.mu NOT held.
func (rf *Raft) replicateToPeer(peer int) {
	rf.mu.Lock()

	// Check if still leader
	if rf.curLeader != rf.me {
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndexes[peer]
	firstIdx := rf.firstLogIndex()

	// If the follower is too far behind, send snapshot
	if nextIdx <= firstIdx && rf.snapshot != nil {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderID:          rf.me,
			LastIncludedIndex: firstIdx,
			LastIncludedTerm:  rf.logs[0].Term,
			Data:              rf.snapshot,
		}
		rf.mu.Unlock()

		reply := &InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			return
		}
		if rf.curLeader != rf.me {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.curLeader = -1
			rf.persist()
			return
		}
		// On success, update nextIndex and matchIndex
		rf.nextIndexes[peer] = firstIdx + 1
		rf.matchIndexes[peer] = firstIdx
		return
	}

	// Send AppendEntries
	var prevLogIdx TLogIndex
	if nextIdx <= 1 {
		prevLogIdx = 0
	} else {
		prevLogIdx = nextIdx - 1
	}
	prevLogTerm := rf.logAt(prevLogIdx).Term

	// Build entries slice
	var entries []Log
	lastLogIdx := rf.lastLogIndex()
	if nextIdx <= lastLogIdx {
		startPos := nextIdx - firstIdx
		endPos := lastLogIdx - firstIdx + 1
		// Copy entries to avoid holding reference to logs under lock
		entries = make([]Log, endPos-startPos)
		copy(entries, rf.logs[startPos:endPos])
	}

	args := &AppendEntriesArgs{
		Term:            rf.currentTerm,
		LeaderID:        rf.me,
		PrevLogIndex:    prevLogIdx,
		PrevLogTerm:     prevLogTerm,
		Entries:         entries,
		LeaderCommitIdx: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		return
	}
	if rf.curLeader != rf.me {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.curLeader = -1
		rf.persist()
		return
	}

	if reply.Success {
		// Update matchIndex and nextIndex
		newMatchIdx := prevLogIdx + TLogIndex(len(entries))
		if newMatchIdx > rf.matchIndexes[peer] {
			rf.matchIndexes[peer] = newMatchIdx
		}
		rf.nextIndexes[peer] = newMatchIdx + 1

		// Try to advance commit index after each successful replication
		rf.advanceCommit()
	} else {
		// Handle conflict
		if reply.LastLogIndex > 0 && reply.LastLogIndex < prevLogIdx {
			// Follower's log is shorter
			rf.nextIndexes[peer] = reply.LastLogIndex + 1
		} else if reply.ConflictTerm > 0 {
			// Find the first entry of the conflicting term
			// Search backwards in our log to find the last entry with ConflictTerm
			found := false
			conflictIdx := reply.ConflictIndex
			if conflictIdx < firstIdx {
				conflictIdx = firstIdx
			}
			// Find the last entry in our log with this term
			for i := rf.lastLogIndex(); i >= firstIdx; i-- {
				if rf.logAt(i).Term == reply.ConflictTerm {
					rf.nextIndexes[peer] = i + 1
					found = true
					break
				}
			}
			if !found {
				rf.nextIndexes[peer] = conflictIdx
			}
		} else {
			// Simple case: just decrement
			if rf.nextIndexes[peer] > firstIdx+1 {
				rf.nextIndexes[peer]--
			}
		}
	}
}

// advanceCommit checks if a majority of matchIndexes have reached a new
// commit point and advances commitIndex. Must be called with rf.mu held.
func (rf *Raft) advanceCommit() {
	if rf.curLeader != rf.me {
		return
	}

	// Collect matchIndexes including our own last log index
	match := make([]int, rf.peersCnt)
	for i := 0; i < rf.peersCnt; i++ {
		if i == rf.me {
			match[i] = int(rf.lastLogIndex())
		} else {
			match[i] = int(rf.matchIndexes[i])
		}
	}
	sort.Ints(match)

	// Majority threshold
	majority := rf.peersCnt/2 + 1
	n := match[rf.peersCnt-majority] // the nth largest matchIndex

	if TLogIndex(n) > rf.commitIndex {
		// Only commit entries from current term (§5.4.2)
		if rf.logAt(TLogIndex(n)).Term == rf.currentTerm {
			rf.commitIndex = TLogIndex(n)
		}
	}
}

// --- Heartbeat (keepalive) ---

func (rf *Raft) keepalive() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.curLeader != rf.me {
		return
	}

	// Send heartbeats to all peers in parallel
	for i := 0; i < rf.peersCnt; i++ {
		if i == rf.me {
			continue
		}

		// For heartbeat, use nextIndex[i] - 1 as PrevLogIndex
		nextIdx := rf.nextIndexes[i]
		firstIdx := rf.firstLogIndex()
		var prevIdx TLogIndex
		if nextIdx <= 1 {
			prevIdx = 0
		} else {
			prevIdx = nextIdx - 1
		}
		if prevIdx < firstIdx {
			prevIdx = firstIdx
		}

		args := &AppendEntriesArgs{
			Term:            rf.currentTerm,
			LeaderID:        rf.me,
			PrevLogIndex:    prevIdx,
			PrevLogTerm:     rf.logAt(prevIdx).Term,
			Entries:         nil,
			LeaderCommitIdx: rf.commitIndex,
		}

		go func(peer int, a *AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, a, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.curLeader != rf.me {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.curLeader = -1
				rf.persist()
				return
			}
			if !reply.Success {
				// Backtrack nextIndex for this peer
				if reply.LastLogIndex > 0 && reply.LastLogIndex < rf.nextIndexes[peer]-1 {
					rf.nextIndexes[peer] = reply.LastLogIndex + 1
				} else if rf.nextIndexes[peer] > rf.firstLogIndex()+1 {
					rf.nextIndexes[peer]--
				}
			}
		}(i, args)
	}
}

// --- Apply Loop (3B) ---

// applyLoop sends committed log entries to the applyCh.
func (rf *Raft) applyLoop() {
	for !rf.killed() {
		rf.mu.Lock()

		// Apply any committed entries that haven't been applied yet
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.logAt(rf.lastApplied)
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Data,
				CommandIndex: int(rf.lastApplied),
			}
			rf.mu.Unlock()

			// Send on applyCh (may block if channel is full)
			rf.appChan <- msg

			rf.mu.Lock()
		}

		rf.mu.Unlock()

		// Sleep a bit to avoid busy-waiting
		time.Sleep(10 * time.Millisecond)
	}
}

// --- Election ---

func (rf *Raft) checkTimeoutAndVoteSelf() {
	rf.mu.Lock()
	if rf.leaderAlive || rf.curLeader == rf.me {
		rf.leaderAlive = false
		rf.mu.Unlock()
		return
	}

	// Start election
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	myTerm := rf.currentTerm
	latestLog := rf.logs[len(rf.logs)-1]
	args := RequestVoteArgs{
		Term:         myTerm,
		CandidateId:  rf.me,
		LastLogIndex: latestLog.Index,
		LastLogTerm:  latestLog.Term,
	}
	peersCnt := rf.peersCnt
	rf.mu.Unlock() // Release lock for RPC phase

	replies := make([]RequestVoteReply, peersCnt)

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var count atomic.Int32
	var higherTerm atomic.Int32
	higherTerm.Store(-1)

	count.Add(1) // vote for self

	for i := 0; i < peersCnt; i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch := make(chan bool, 1)
			go func() {
				ok := rf.sendRequestVote(i, &args, &(replies[i]))
				ch <- ok
			}()
			select {
			case ok := <-ch:
				if !ok {
					return
				}
				if replies[i].Term > myTerm {
					t := int32(replies[i].Term)
					for {
						cur := higherTerm.Load()
						if t <= cur || higherTerm.CompareAndSwap(cur, t) {
							break
						}
					}
					return
				}
				if replies[i].VoteGranted {
					count.Add(1)
				}
			case <-ctx.Done():
				return
			}
		}(i)
	}
	wg.Wait()

	// Re-acquire lock to finalize election
	rf.mu.Lock()
	defer rf.mu.Unlock()

	ht := higherTerm.Load()
	if ht != -1 {
		// Found a higher term, step down
		if TTermIndex(ht) > rf.currentTerm {
			rf.currentTerm = TTermIndex(ht)
		}
		rf.votedFor = -1
		rf.curLeader = -1
		rf.persist()
		return
	}

	// Check if our term is still current (could have changed via RPCs)
	if myTerm != rf.currentTerm {
		return
	}

	if int(count.Load()) > peersCnt/2 {
		rf.curLeader = rf.me
		rf.persist()

		// Initialize leader state
		lastIdx := rf.lastLogIndex()
		for idx := 0; idx < peersCnt; idx++ {
			rf.nextIndexes[idx] = lastIdx + 1
			rf.matchIndexes[idx] = 0
		}

		// Signal replicator to start working
		go func() {
			select {
			case rf.replicateCh <- struct{}{}:
			default:
			}
		}()
	}
}

// --- Ticker goroutines ---

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.checkTimeoutAndVoteSelf()

		ms := 500 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// leaderTicker sends heartbeats and triggers replication.
func (rf *Raft) leaderTicker() {
	for !rf.killed() {
		rf.keepalive()
		time.Sleep(100 * time.Millisecond)
	}
}

// replicateLoop triggers replication to followers when there are new entries.
func (rf *Raft) replicateLoop() {
	for !rf.killed() {
		select {
		case <-rf.replicateCh:
			// Triggered by Start()
		case <-time.After(50 * time.Millisecond):
			// Periodic trigger
		}

		rf.mu.Lock()
		if rf.curLeader != rf.me {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		// Replicate to all peers
		for i := 0; i < rf.peersCnt; i++ {
			if i == rf.me {
				continue
			}
			go rf.replicateToPeer(i)
		}

		// After replication, try to advance commit index
		rf.mu.Lock()
		rf.advanceCommit()
		rf.mu.Unlock()
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// --- Make: create a new Raft peer ---

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.peersCnt = len(rf.peers)
	rf.leaderAlive = true // initially assume leader exists; timer will reset if not
	rf.curLeader = -1
	rf.votedFor = -1

	// Initialize log with a sentinel entry at index 0
	rf.logs = make([]Log, 1)
	rf.logs[0] = Log{Term: 0, Index: 0}

	rf.nextIndexes = make([]TLogIndex, rf.peersCnt)
	rf.matchIndexes = make([]TLogIndex, rf.peersCnt)

	rf.appChan = applyCh
	rf.replicateCh = make(chan struct{}, 10)

	// Restore persisted state
	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		rf.snapshot = snapshot
	}
	rf.readPersist(persister.ReadRaftState())

	// Start goroutines
	go rf.ticker()
	go rf.leaderTicker()
	go rf.replicateLoop()
	go rf.applyLoop()

	return rf
}
