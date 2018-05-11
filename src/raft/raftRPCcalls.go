package raft

/*----------------------------------------------------------*/
/*this files is for the leader to send RPC call to each Raft*/
/*----------------------------------------------------------*/

//AppendEntries apply log entries from leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("The num-%v receive a rpc heartbeatcall & send himself HB\n", rf.me)
	DPrintf("The num-%v receive a HB-{term-%v,leaderId-%v}", rf.me, args.Term, args.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	//old leader "sending" args
	//raft with recognization of the new leader "receive" this args
	//the old leader(failure or delay so there is a another true leader now)
	//the old leader send the request args with lesser than follower's currentTerm
	//just return the upToDate term to the fake old leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.nextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	//new leader first time "sending" heartbeat to follower/candidate who is normal or wake up from a failure/delay
	//now the rf is a follower or candidator
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
	}

	//using the heartbeat channel to pass the heartbeat message to raft runServer() goroutine
	rf.heartbeat <- true
	reply.Term = rf.currentTerm

	//if the logs from leaders is incomplete for the current raft than return the reply to get a complete logs for current raft
	//return the failure reply for leader so the leader will decrement nextIndex and retry
	//add the nextIndex to optimize the retry times
	/*	incomplete: leader is trying to append index-6 but the follower last logs index is 3
		index:			012345
		leader-logs:	112223
		follow-logs:	112
	*/
	//set the prevLogIndex to the nextIndex of rf.logs
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.nextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	//now the nextTryIndex == rf.getLastLogIndex() +1 == 5 and args.PrevLogIndex == rf.getLastLogIndex() == 4
	//and start to detect the conflict as example below
	//when prevLogIndex <= 0, it means the logs is empty
	//todo: is the args.PrevLogIndex > 0 or args.PrevLogIndex >= 0
	//the args.entries is not enough for solve the conflict
	/*
			index			0123456
			leader-logs:l1:	1133445
			follow-logs:l2:	11225
		the if-block below detect whether the l1[4].term == l2[4].term
		if equal than only needs to replicate the succeeding logEntries
		if unequal than needs more logEntires to replicate
	*/
	if args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		//needs more logEntires to replicate
		//term == 5
		term := rf.logs[args.PrevLogIndex].Term

		for reply.nextTryIndex = args.PrevLogIndex - 1; reply.nextTryIndex > 0 && rf.logs[reply.nextTryIndex].Term == term; reply.nextTryIndex-- {
		}

		reply.nextTryIndex++
	} else {
		//only needs to replicate the succeeding logEntries
		//split
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rest := rf.logs[args.PrevLogIndex+1:]
		if conflicted(rest, args.Entries) || len(args.Entries) > len(rest) {
			//conflicted or follower len lesser than leader's-just overwrite the logs
			/*
				args.entries:	33445
				rest1:			2244	1||1	result:	33445
				rest2:			22445	1||0	result:	33445
				rest3:			3344	0||1	result:	33445
			*/
			rf.logs = append(rf.logs, args.Entries...)
		} else {
			//no conflicted and the length of args.entries is lesser than follower's
			//just let the follower's logs length greater than leader's since it hasn't been commited and will be overwrite after the leader's args.Enties larger than follower's logs
			/*
				args.entries:	33445
				rest1:			334456	0||0	result:	334456
			*/
			rf.logs = append(rf.logs, rest...)
		}

		//successfully append entries
		reply.Success = true
		reply.nextTryIndex = args.PrevLogIndex

		// update follower's commitIndex if no conflict
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit <= rf.getLastLogIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.getLastLogIndex()
			}

			go rf.commitLogs()
		}
	}
}

//for server commit its logs
func (rf *Raft) commitLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{CommandIndex: i, Command: rf.logs[i].Command}
	}

	rf.lastApplied = rf.commitIndex
}

//detect whether there is a conflict between follower's logs and leader's logs
//flwrLogs-follower's logs	ldrLogs-leader's logs
func conflicted(flwrLogs []LogEntry, ldrLogs []LogEntry) bool {
	for i := range flwrLogs {
		//dont't let the ldrLogs access a possition out of bound
		if i >= len(ldrLogs) {
			break
		}
		if flwrLogs[i].Term != ldrLogs[i].Term {
			return true
		}
	}
	return false
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//todo: update the information based on reply
	return ok
}

func (rf *Raft) sendAllAppendEntries() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//each raft instance should receive different args
	for i := range rf.peers {
		//only when the rf is still the leader, the leader raft send appendEntries request
		if i != rf.me && rf.status == Leader {
			//create the append args
			args := new(AppendEntriesArgs)
			args.Term = rf.currentTerm
			args.LeaderId = rf.me

			//if the logs is empty:	prevLogIndex == -1
			args.PrevLogIndex = rf.getLastLogIndex() - 1

			//the logs isn't empty so the prevLogTerm can be found in the logs
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			} else {
				//printout the prevLogTerm when the args.PrevLogIndex = -1
				Trace("raft-%v:	The args.prevlogTerm < 0 which means the logs is empty happen in the sendAllAppendEntries() and the PrevLogTerm==%v ", rf.me, args.PrevLogTerm)
			}
			//when the nextIndex of follower logs is lesser than leader.nextIndex, it means that the follower's log is incomplete
			//when nextIndex greater than lastLogIndex, it means the follower's logs is up to date and the entries is a empty slice
			if rf.nextIndex[i] <= rf.getLastLogIndex() {
				args.Entries = rf.logs[rf.nextIndex[i]:]
			}

			DPrintf("---Num-%v raft Creating a appendEntries request-{term-%v,leaderId-%v,prevLogTerm-%v,prevLogIndex-%v,entries-%v}", rf.me, args.Term, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex, args.Entries)

			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
}

//-----------------------heartbeat rpc sta----------------------
//wrap the sendAppendEntries to heartbeat sending function
func (rf *Raft) sendAllHeartbeat() {
	DPrintf("num-%v sendding heartbeat", rf.me)
	rf.sendAllAppendEntries()
}

//-----------------------vote request rpc sta----------------------

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

	//the rf become a leader so we don't need the rf.electWin <- below
	//eleciton timeout
	//start a new candidate proccess
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
