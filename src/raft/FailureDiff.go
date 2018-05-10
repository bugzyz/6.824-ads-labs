package raft

//bad
func (rf *Raft) AppendEntries1(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("The num-%v receive a rpc heartbeatcall & send himself HB\n", rf.me)
	DPrintf("The num-%v receive a HB-{term-%v,leaderId-%v}", rf.me, args.Term, args.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	if rf.currentTerm <= args.Term {
		rf.heartbeat <- true
		reply.Success = true
		reply.Term = args.Term

		//update the receiver raft status
		rf.status = Follower
		rf.currentTerm = args.Term

		DPrintf("The num-%v switch back to follower now{term-%v}\n", rf.me, rf.currentTerm)
	} else {
		DPrintf("The num-%v have a greater term(args:%v < rf:%v) than HB\n", rf.me, args.Term, rf.currentTerm)

	}
}

//good
func (rf *Raft) AppendEntries2(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
