package raft

/*----------------------------------------------------------*/
/*this files is for the leader to send RPC call to each Raft*/
/*----------------------------------------------------------*/

//send appendEntry to each raft
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("The num-%v receive a rpc heartbeatcall & send himself HB\n", rf.me)
	DPrintf("The num-%v receive a HB-{term-%v,leaderId-%v}", rf.me, args.Term, args.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}

	rf.heartbeat <- true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendAllAppendEntries() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := new(AppendEntriesArgs)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	DPrintf("---Num-%v raft Creating a appendEntries request-{term-%v,leaderId-%v}", rf.me, args.Term, args.LeaderId)

	for i := range rf.peers {
		if i != rf.me && rf.status == Leader {
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
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
	DPrintf("num-%v sendding heartbeat", rf.me)
	rf.sendAllAppendEntries()
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

	//the rf become a leader\eleciton timeout and start a new candidate proccess
	if rf.status != Candidate || args.Term != rf.currentTerm {
		return ok
	}

	//successfully return ok:true
	//but return a greater term
	if reply.Term > rf.currentTerm {
		DPrintf("rf-%v receive a term greater than rf-curTerm reply:%v rf-cuTem:%v", rf.me, reply.Term, rf.currentTerm)

		Warning("Candidate raft-%v:%v receive a bigger term-%v and become follower", rf.me, rf.currentTerm, reply.Term)
		rf.status = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		return ok
	}
	DPrintf("rf-%v receive a voteRequest reply:%v", rf.me, reply.VoteGranted)
	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			Success("Candidate raft-%v:%v become leader with vote:%v", rf.me, rf.currentTerm, rf.voteCount)
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
