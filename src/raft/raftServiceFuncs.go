package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) runServer() {
	DPrintf("The num-%v raft server start election goroutine\n", rf.me)
	for {
		switch rf.status {
		case Leader:
			DPrintf("Raft-%v switch-leader: term-%v\n", rf.me, rf.currentTerm)
			//sending heartbeat to follower
			rf.sendAllHeartbeat()
			time.Sleep(time.Millisecond * 120)

		case Follower:
			DPrintf("Raft-%v switch-follower: term-%v\n", rf.me, rf.currentTerm)
			select {
			//receive a vote request
			case <-rf.granted:
			//receive heartbeat
			case <-rf.heartbeat:
			//timeout
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+300)):
				rf.status = Candidate
			}

		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			DPrintf("Raft-%v switch-candidate: term-%v\n", rf.me, rf.currentTerm)
			rf.votedFor = rf.me
			rf.persist()
			rf.voteCount = 1
			rf.mu.Unlock()
			rf.sendAllRequestVotes()

			//todo: run a goroutine that if the votes > peers/2 than the candidate become a leader
			go rf.checkElectionWin()

			select {
			//election timeout and try again
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+300)):
			//other become the leader and start to send heartbeat
			case <-rf.heartbeat:
				rf.status = Follower
			//election success
			case <-rf.electWin:
				rf.status = Leader
			}
		}
	}
}

//to do: the goroutine function for checking whether rf's election win
func (rf *Raft) checkElectionWin() {

}

func (rf *Raft) candidatesLogIsUp2Date(argsTerm int, argsIndex int) bool {
	rfLastTerm := rf.getLastLogTerm()
	rfLastIndex := rf.getLastLogIndex()

	if argsTerm != rfLastTerm {
		return argsTerm > rfLastTerm
	}
	DPrintf("check up2date is %v", argsIndex >= rfLastIndex)
	return argsIndex >= rfLastIndex

}
