package raft

import "fmt"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

func (request RequestVoteArgs) String() string {
	return fmt.Sprintf("{term: %d, candidateId: %d, lastLogIndex: %d, lastLogTerm: %d}", request.Term, request.CandidateId, request.LastLogIndex, request.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("term: %d, voteGranted: %t", reply.Term, reply.VoteGranted)
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func (request AppendEntriesRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,PrevLogIndex:%v,PrevLogTerm:%v,Entries:%v,LeaderCommit:%v}", request.Term, request.LeaderId, request.PrevLogIndex, request.PrevLogTerm, request.Entries, request.LeaderCommit)
}

type AppendEntriesReply struct {
	Term           int
	Success        bool
	LastMatchIndex int
	LastMatchTerm  int
}

func (reply AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%v,Success:%v,LastMatchIndex:%v,LastMatchTerm:%v}", reply.Term, reply.Success, reply.LastMatchIndex, reply.LastMatchTerm)
}

type InstallSnapshotRequest struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

func (request InstallSnapshotRequest) String() string {
	return fmt.Sprintf("{Term:%v,LeaderId:%v,LastIncludeIndex:%v,LastIncludeTerm:%v}", request.Term, request.LeaderId, request.LastIncludeIndex, request.LastIncludeTerm)
}

type InstallSnapshotReply struct {
	Term int
}

func (reply InstallSnapshotReply) String() string {
	return fmt.Sprintf("{Term:%v}", reply.Term)
}
