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
	Term         int
	Success      bool
	NextTryIndex int
}

//-------appendEntry end-------

//-------snapshotArgs sta-------
type InstallSsArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int

	// add? As the Hint on 6.824: You do not have to implement Figure 13's offset mechanism for splitting up the snapshot.
	// Offset int

	//raw bytes of the snapshot chunk
	Data []byte

	// add? If it is neccessary to send several time to complete the snapshot copying?
	// done bool
}

type InstallSsReply struct {
	Term int
}
