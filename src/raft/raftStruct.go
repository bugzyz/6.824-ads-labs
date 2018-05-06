package raft

//raft state
const (
	Follower = iota
	Candidate
	Leader
)

//LogEntry -the data structure for storing command
type LogEntry struct {
	Term    int
	Command interface{}
}

//-------appendEntry sta-------
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	//nextTryIndex int
}

//-------appendEntry end-------
