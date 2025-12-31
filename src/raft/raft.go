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

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

/**
 * The following code is about the Raft object and its basic methods.
 */
// A Go object implementing a single Raft peer.
type Raft struct {
	mu               sync.RWMutex        // Lock to protect shared access to this peer's state
	peers            []*labrpc.ClientEnd // RPC end points of all peers
	persister        *Persister          // Object to hold this peer's persisted state
	me               int                 // this peer's index into peers[]
	dead             int32               // set by Kill()
	currentTerm      int
	votedFor         int
	state            NodeState
	heartbeatTimeout *time.Timer
	electionTimeout  *time.Timer
	applyCh          chan ApplyMsg
	logs             []LogEntry
	commitIndex      int
	lastApplied      int
	matchIndex       []int
	nextIndex        []int //Sometimes, there are conflicts between the logs of the leader and the followers. The leader needs adjust the nextIndex and send the logs again.
	applyCond        *sync.Cond
	replicatorCond   []*sync.Cond
	// snapApplyCount           int
	waitApplySnapshotRequest InstallSnapshotRequest
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		dead:             0,
		currentTerm:      0,
		votedFor:         -1,
		state:            Follower,
		heartbeatTimeout: time.NewTimer(time.Duration(StableHeartbeatTimeout())),
		electionTimeout:  time.NewTimer(time.Duration(RandomizedElectionTimeout())),
		applyCh:          applyCh,
		logs:             make([]LogEntry, 1),
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		replicatorCond:   make([]*sync.Cond, len(peers)),
		commitIndex:      0,
		lastApplied:      0,
		// snapApplyCount:           0,
		waitApplySnapshotRequest: InstallSnapshotRequest{Term: -1},
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLog.Index + 1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}
	// start ticker goroutine to start elections
	if me == 0 {
		Debug(dInfo, "peerNum %v", len(peers))
	}
	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	// Your code here (2B).
	newLog := rf.appendNewLog(command)
	Debug(dLog, "S%v(leader) receives a new log entry {%v}", rf.me, newLog)
	rf.broadcastHeartbeat(false)
	return newLog.Index, newLog.Term, true
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) appendNewLog(command interface{}) LogEntry {
	lastLog := rf.getLastLog()
	newLog := LogEntry{lastLog.Index + 1, rf.currentTerm, command}
	rf.logs = append(rf.logs, newLog)
	rf.persist()
	rf.matchIndex[rf.me] = newLog.Index
	rf.nextIndex[rf.me] = newLog.Index + 1
	return newLog
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) getFirstIndex() int {
	return rf.logs[0].Index
}

/**
 * The following code is the ticker function.
 * It is about the election and heartbeat ticker.
 */
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.heartbeatTimeout.C:
			rf.mu.Lock()
			if rf.state == Leader {
				Debug(dTimer, "S%d Leader, the heartbeat timer timeout", rf.me)
				rf.broadcastHeartbeat(true)
				rf.heartbeatTimeout.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		case <-rf.electionTimeout.C:
			rf.mu.Lock()
			Debug(dTimer, "S%d {state: %v, term: %v}, the election timer timeout", rf.me, rf.state, rf.currentTerm)
			rf.changeState(Candidate)
			rf.currentTerm++
			rf.persist()
			rf.startElection()
			rf.electionTimeout.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()
		}
	}
}

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

const (
	HeartbeatTimeout = 125
	ElectionTimeout  = 600
)

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}

/**
 * The following code is about the election process.
 */
func (rf *Raft) startElection() {
	request := rf.genRequestVoteRequest()
	Debug(dVote, "S%v starts election with RequestVoteRequest %v", rf.me, request)
	rf.votedFor = rf.me
	rf.persist()
	grantedVoteNum := 1

	// Your code here (2A, 2B).
	for peer := range rf.peers {
		if peer != rf.me {
			go rf.electionRequestOnce(peer, &grantedVoteNum, request)
		}
	}
}

func (rf *Raft) electionRequestOnce(peer int, grantedVoteNum *int, request *RequestVoteArgs) {
	reply := new(RequestVoteReply)
	if rf.sendRequestVote(peer, request, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dVote, "S%v received RequestVoteReply {%v} from S%v", rf.me, reply, peer)
		if rf.currentTerm == request.Term && rf.state == Candidate {
			if reply.VoteGranted {
				*grantedVoteNum++
				if *grantedVoteNum > len(rf.peers)/2 {
					rf.changeState(Leader)
				}
			}
		} else if reply.Term > rf.currentTerm {
			Debug(dVote, "S%v found higher term %v in RequestVoteReply %v from S%v", rf.me, reply.Term, reply, peer)
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			rf.changeState(Follower)
		}
	}
}

func (rf *Raft) changeState(newState NodeState) {
	if rf.state == newState {
		return
	}
	Debug(dInfo, "S%v changes state from %s to %s", rf.me, rf.state, newState)
	rf.state = newState

	switch newState {
	case Follower:
		rf.heartbeatTimeout.Stop()
		rf.electionTimeout.Reset(RandomizedElectionTimeout())
	case Candidate:
	case Leader:
		lastLog := rf.getLastLog()
		for i := range rf.peers {
			rf.nextIndex[i] = lastLog.Index + 1
			rf.matchIndex[i] = 0
		}
		rf.broadcastHeartbeat(true)
		rf.heartbeatTimeout.Reset(StableHeartbeatTimeout())
		rf.electionTimeout.Stop()
	}
}

func (rf *Raft) genRequestVoteRequest() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// used by RequestVote Handler to judge which log is newer
func (rf *Raft) isLogUpToDate(term, index int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer Debug(dVote, "S%v state is {state: %v, term: %v}, the RequestVoteReply is {%v}", rf.me, rf.state, rf.currentTerm, reply)

	if request.Term < rf.currentTerm || (request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
		rf.changeState(Follower)
	}
	if !rf.isLogUpToDate(request.LastLogTerm, request.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.votedFor = request.CandidateId
	rf.electionTimeout.Reset(RandomizedElectionTimeout())
	// now the term of the candidate must equal to the current term of the rf
	reply.Term, reply.VoteGranted = rf.currentTerm, true
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
func (rf *Raft) sendRequestVote(server int, request *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", request, reply)
	return ok
}

/**
 * The following code is about the log replication process and the heartbeat broadcast.
 */
func (rf *Raft) broadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer != rf.me {
			if isHeartbeat {
				Debug(dLeader, "S%v broadcasts heartbeat to S%v", rf.me, peer)
				go rf.replicateOneRound(peer)
			} else {
				rf.replicatorCond[peer].Signal()
			}
		}
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}

	lastSendIndex := rf.nextIndex[peer] - 1

	if lastSendIndex < rf.getFirstIndex() {
		rf.mu.RUnlock()
		installSnapshotRequest := rf.genInstallSnapshotRequest()
		installSnapshotReply := new(InstallSnapshotReply)
		Debug(dLeader, "S%v sends to S%v with InstallSnapshotRequest {%v} ", rf.me, peer, installSnapshotRequest)
		if rf.sendInstallSnapshot(peer, installSnapshotRequest, installSnapshotReply) {
			rf.mu.Lock()
			rf.handleInstallSnapshotReply(peer, installSnapshotRequest, installSnapshotReply)
			rf.mu.Unlock()
		}
	} else {
		request := rf.genAppendEntriesRequest(lastSendIndex)
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		if request.Entries != nil && len(request.Entries) > 0 {
			Debug(dLeader, "S%v sends to S%v with AppendEntriesRequest {%v} ", rf.me, peer, request)
		}
		if rf.sendAppendEntries(peer, request, reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, request, reply)
			rf.mu.Unlock()
			// Debug(dLeader, "S%v received AppendEntriesReply {%v} from S%v", rf.me, reply, peer)
		}
	}
}

// the leader maybe try replicate logs to the follower many times
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) genAppendEntriesRequest(lastSendIndex int) *AppendEntriesRequest {
	entries := make([]LogEntry, rf.getLastLog().Index-lastSendIndex)
	copy(entries, rf.logs[lastSendIndex+1-rf.getFirstIndex():])
	return &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: lastSendIndex,
		PrevLogTerm:  rf.logs[lastSendIndex-rf.getFirstIndex()].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesRequest, reply *AppendEntriesReply) {
	if rf.state != Leader || request.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		if request.Entries != nil && len(request.Entries) > 0 {
			Debug(dLog, "S%v has replicated log entries(startId: %v, length: %v) to S%v. Now the S%v's nextIndex is %v", rf.me, request.PrevLogIndex+1, len(request.Entries), peer, peer, rf.nextIndex[peer])
		} else {
			Debug(dInfo, "S%v has received heartbeat reply from S%v", rf.me, peer)
		}
		rf.updateCommitIndexForLeader()
	} else {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			Debug(dWarn, "S%v found higher term %v in AppendEntriesReply %v from S%v", rf.me, reply.Term, reply, peer)
			rf.changeState(Follower)
		} else if reply.Term == rf.currentTerm {
			if reply.LastMatchTerm == -1 {
				if reply.LastMatchIndex == -1 {
					Debug(dLog, "S%v send an outdated AppendEntriesRequest {%v} to S%v", rf.me, request, peer)
				} else {
					rf.nextIndex[peer] = reply.LastMatchIndex + 1
					Debug(dLog, "S%v failed to replicate log entries to S%v, because S%v's log is too short. Now the S%v's nextIndex is %v", rf.me, peer, peer, peer, rf.nextIndex[peer])
				}
			} else {
				rf.nextIndex[peer] = rf.getFirstIndex()
				for i := reply.LastMatchIndex; i >= rf.getFirstIndex(); i-- {
					if rf.logs[i-rf.getFirstIndex()].Term <= reply.LastMatchTerm {
						rf.nextIndex[peer] = i + 1
						break
					}
				}
				Debug(dWarn, "S%v failed to replicate log entries to S%v, because of different term in %v log(S%v's term: %v, S%v's term %v). now the S%v's nextIndex is %v",
					rf.me, peer, request.PrevLogIndex, rf.me, request.PrevLogTerm, peer, reply.LastMatchTerm, peer, rf.nextIndex[peer])
			}
		}
	}
}

func (rf *Raft) AppendEntries(request *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if request.Entries != nil && len(request.Entries) > 0 {
		Debug(dLog, "S%v received from S%v with AppendEntriesRequest {%v}", rf.me, request.LeaderId, request)
	} else {
		Debug(dInfo, "S%v received heartbeat from S%v", rf.me, request.LeaderId)
	}
	if request.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = request.Term, -1
	}

	rf.changeState(Follower)
	rf.electionTimeout.Reset(RandomizedElectionTimeout())

	if !rf.matchLog(request.PrevLogTerm, request.PrevLogIndex) {
		// return index <= rf.getLastLog().Index && rf.logs[index-rf.getFirstIndex()].Term == term
		if request.PrevLogIndex <= rf.getLastLog().Index {
			Debug(dLog, "S%v received AppendEntriesRequest from S%v, but the log is not match(lastLog: {%v}, preLog: {%v})", rf.me, request.LeaderId, rf.getLastLog(), rf.logs[request.PrevLogIndex-rf.getFirstIndex()])
		} else {
			Debug(dLog, "S%v received AppendEntriesRequest from S%v, but the log is not match(lastLog: {%v})", rf.me, request.LeaderId, rf.getLastLog())
		}
		reply.Term, reply.Success = rf.currentTerm, false
		lastIndex := rf.getLastLog().Index
		if lastIndex < request.PrevLogIndex { // the follower's log is too short
			reply.LastMatchIndex, reply.LastMatchTerm = lastIndex, -1
			Debug(dLog, "S%v failed to append log entries from S%v, because S%v's log is too short(lastIndex: %v).", rf.me, request.LeaderId, rf.me, lastIndex)
		} else {
			// the follower's log with PrevLogIndex is not match the term,
			// it's term is smaller than the term of leader's log with PrevLogIndex.
			// Next time, we need to send the log entries from the leader's log with follower's smaller term.
			reply.LastMatchTerm = rf.logs[request.PrevLogIndex-rf.getFirstIndex()].Term
			for i := request.PrevLogIndex; i >= 0; i-- {
				if rf.logs[i-rf.getFirstIndex()].Term <= request.PrevLogTerm {
					reply.LastMatchIndex = i
					break
				}
			}
			Debug(dLog, "S%v failed to append log entries from S%v, because of different term in %v log(S%v's term: %v, S%v's term %v).",
				rf.me, request.LeaderId, request.PrevLogIndex, rf.me, rf.logs[request.PrevLogIndex-rf.getFirstIndex()].Term, request.LeaderId, request.PrevLogTerm)
		}

		return
	}

	isAppend := false
	for index, entry := range request.Entries {
		// May be there are some log entries already exist in the follower's log. We don't need to copy it again.
		if entry.Index >= rf.getLastLog().Index || (entry.Index-rf.getFirstIndex() >= 0 && rf.logs[entry.Index-rf.getFirstIndex()].Term != entry.Term) { //Question 需要大于等于0，因为有可能因为网络延迟延迟发送过来，而这时commitId已经因为Snap更新了
			rf.logs = append(rf.logs[:entry.Index-rf.getFirstIndex()], request.Entries[index:]...)
			Debug(dLog, "S%v appends log entries(startId: %v, length: %v) from S%v", rf.me, entry.Index, rf.getLastLog().Index-entry.Index+1, request.LeaderId)
			isAppend = true
			break
		}
	}
	if !isAppend && len(request.Entries) > 0 {
		Debug(dLog, "S%v received duplicated log entries from S%v", rf.me, request.LeaderId)
	}

	rf.updateCommitIndexForFollower(request.LeaderCommit)

	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) matchLog(term, index int) bool {
	return index < rf.getFirstIndex() || (index <= rf.getLastLog().Index && rf.logs[index-rf.getFirstIndex()].Term == term)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) genInstallSnapshotRequest() *InstallSnapshotRequest {
	return &InstallSnapshotRequest{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.getFirstIndex(),
		LastIncludeTerm:  rf.logs[0].Term,
		Data:             rf.persister.ReadSnapshot(),
	}
}

func (rf *Raft) handleInstallSnapshotReply(peer int, request *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	if reply.Term > rf.currentTerm {
		rf.changeState(Follower)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		Debug(dWarn, "S%v found higher term %v in InstallSnapshotReply %v from S%v, changes to follower", rf.me, reply.Term, reply, peer)
	} else {
		rf.nextIndex[peer] = request.LastIncludeIndex + 1
		rf.matchIndex[peer] = request.LastIncludeIndex
		Debug(dLog, "S%v has installed snapshot to S%v, now the S%v's nextIndex is %v", rf.me, peer, peer, rf.nextIndex[peer])
		rf.updateCommitIndexForLeader()
	}
}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	Debug(dSnap, "S%v {term: %v, commitIndex: %v}, received from S%v with InstallSnapshotRequest {%v} ", rf.me, rf.currentTerm, rf.commitIndex, request.LeaderId, request)
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if request.Term < rf.currentTerm {
		return
	}
	if request.Term > rf.currentTerm {
		rf.currentTerm = request.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.changeState(Follower)

	if request.LastIncludeIndex <= rf.commitIndex {
		return
	}

	rf.persister.Save(rf.encodeState(), request.Data)
	rf.commitIndex = request.LastIncludeIndex
	rf.logs = []LogEntry{{request.LastIncludeIndex, request.LastIncludeTerm, nil}} //2D遇到的bug所在
	Debug(dSnap, "S%v installs snapshot from S%v, now the commitIndex is %v", rf.me, request.LeaderId, rf.commitIndex)

	rf.waitApplySnapshotRequest = *request
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) updateCommitIndexForLeader() {
	n := len(rf.matchIndex)
	srt := make([]int, n)
	copy(srt, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(srt)))
	newCommitIndex := srt[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		// Only when the log of the leader's current term is accepted by the majority of people can it be submitted.
		// You cannot submit the logs of other terms separately in advance.
		if rf.matchLog(rf.currentTerm, newCommitIndex) {
			Debug(dLog, "S%v(leader) updates commitIndex from %v to %v, try to signal applyCond", rf.me, rf.commitIndex, newCommitIndex)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		} else {
			Debug(dWarn, "S%v cannot commit log{%v} at index %v because it does not match the current term %v, rf.commitIndex: %v, srt: %v",
				rf.me, rf.logs[newCommitIndex-rf.getFirstIndex()], newCommitIndex, rf.currentTerm, rf.commitIndex, srt)
		}
	}

}

func (rf *Raft) updateCommitIndexForFollower(leaderCommit int) {
	newCommitIndex := Min(leaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		Debug(dLog, "S%v updates commitIndex from %v to %v, try to signal applyCond", rf.me, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		if rf.waitApplySnapshotRequest.Term != -1 {
			if rf.lastApplied < rf.waitApplySnapshotRequest.LastIncludeIndex {
				rf.mu.Unlock()

				rf.applyCh <- ApplyMsg{ //Question: two applyCh update way, how to update orderly?
					SnapshotValid: true,
					Snapshot:      rf.waitApplySnapshotRequest.Data,
					SnapshotTerm:  rf.waitApplySnapshotRequest.LastIncludeTerm,
					SnapshotIndex: rf.waitApplySnapshotRequest.LastIncludeIndex,
				}

				rf.mu.Lock()
				rf.lastApplied = rf.waitApplySnapshotRequest.LastIncludeIndex
				Debug(dSnap, "S%v applies snapshot from S%v, now the lastApplied is %v", rf.me, rf.waitApplySnapshotRequest.LeaderId, rf.lastApplied)

			}
			rf.waitApplySnapshotRequest = InstallSnapshotRequest{Term: -1}
			rf.mu.Unlock()
		} else {
			commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
			if rf.getFirstIndex() != 0 && lastApplied+1-rf.getFirstIndex() <= 0 {
				Debug(dWarn, "S%v has no log to apply, because lastApplied %v < firstIndex %v", rf.me, lastApplied, rf.getFirstIndex())
				rf.mu.Unlock()
				continue
			}
			entries := make([]LogEntry, commitIndex-lastApplied)
			Debug(dInfo, "S%v pre to apply log entries. LastApplied: %v, FirstIndex: %v, commitIndex: %v)",
				rf.me, lastApplied, rf.getFirstIndex(), commitIndex)
			copy(entries, rf.logs[lastApplied+1-rf.getFirstIndex():commitIndex+1-rf.getFirstIndex()])
			rf.mu.Unlock()

			for _, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
					CommandTerm:  entry.Term,
				}
			}

			rf.mu.Lock()
			Debug(dInfo, "S%v finishes applying log entries(startId: %v, length: %v), now rf.lastApplied = %v",
				rf.me, lastApplied+1, len(entries), rf.lastApplied)
			rf.lastApplied = commitIndex
			rf.mu.Unlock()
		}
	}
}

/**
 * The following code is about the persistent state.
 */
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
	rf.persister.Save(rf.encodeState(), rf.persister.snapshot)
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()

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
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		Debug(dError, "S%v failed to read persist", rf.me)
	} else {
		Debug(dInfo, "S%v read persist successfully", rf.me)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastApplied = rf.getFirstIndex()
		rf.commitIndex = rf.getFirstIndex()
	}
}

/**
* The following code is about the snapshot.
 */
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.getFirstIndex() {
		Debug(dSnap, "S%v ignores the snapshot request with end index %v, because the index is not bigger than the first index %v", rf.me, index, rf.getFirstIndex())
		return
	}

	rf.logs = append([]LogEntry{{index, rf.logs[index-rf.getFirstIndex()].Term, nil}}, rf.logs[index-rf.getFirstIndex()+1:]...)
	rf.persister.Save(rf.encodeState(), snapshot)
	Debug(dSnap, "S%v applies the snapshot with end index %v, now the len(logs)=%v", rf.me, index, len(rf.logs))
}
