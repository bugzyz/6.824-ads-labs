package raft

/*----------------------------------------------------------*/
/*this files is for the leader to send RPC call to each Raft*/
/*----------------------------------------------------------*/

//send appendEntry to each raft
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.heartbeat <- true
}

func (rf *Raft) sendAppendEntries(server int) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", new(AppendEntriesArgs), new(AppendEntriesReply))
	return ok
}

func (rf *Raft) sendAllAppendEntries() {
	for i := range rf.peers {
		if i != rf.me && rf.status == Leader {
			go rf.sendAppendEntries(i)
		}
	}
}

//-----------------------heartbeat rpc sta----------------------

//for the RPC to make each raft get heartbeat
func (rf *Raft) ReceiveHB(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.heartbeat <- true
	return true
}

func (rf *Raft) sendHeartbeat(server int) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", new(AppendEntriesArgs), new(AppendEntriesReply))
	return ok
}

func (rf *Raft) sendAllHeartbeat() {
	for i := range rf.peers {
		if i != rf.me && rf.status == Leader {
			go rf.sendHeartbeat(i)
		}
	}
}

//-----------------------heartbeat rpc end----------------------

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
			go rf.sendRequestVoteAndDetectElectionWin(serverNum, args, new(RequestVoteReply))
		}
	}
}

//A detection function including the sendRequestVote function
//rf-the raft that send vote request
func (rf *Raft) sendRequestVoteAndDetectElectionWin(serverNum int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//this function will wait until the reply is filled
	ok := rf.sendRequestVote(serverNum, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//return the failed ok
	if !ok {
		return ok
	}
	//successfully return ok:true
	//but return a greater term
	if reply.Term > rf.currentTerm {
		rf.status = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		return ok
	}
	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.status = Leader
			rf.electWin <- true
		}
	}
	return ok
}

//get last log's term
func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()].Term
}

//return the last log index
func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}
