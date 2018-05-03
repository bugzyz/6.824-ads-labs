package raft

/*----------------------------------------------------------*/
/*this files is for the leader to send RPC call to each Raft*/
/*----------------------------------------------------------*/

//send appendEntry to each raft
func (rf *Raft) sendAllAppendEntries() {
}

//send votes request to each raft
func (rf *Raft) sendAllRequestVotes() {
	rf.mu.Lock()
	//init request vote args
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()

	//because the rpc call is a waiting call so use a goroutine to call
	for serverNum := range rf.peers {
		if serverNum != rf.me && rf.status == Candidate {
			go rf.sendRequestVote(serverNum, args, new(RequestVoteReply))
		}
	}
}

//get last log's term
func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()].Term
}

//return the last log index
func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}
