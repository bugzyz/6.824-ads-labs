package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) runServer() {
	for {
		switch rf.status {
		case Leader:
			Info("Raft-%v switch-leader: term-%v", rf.me, rf.currentTerm)
			//sending heartbeat to follower
			rf.sendAllHeartbeat()
			time.Sleep(time.Millisecond * 120)

		case Follower:
			Info("Raft-%v switch-follower: term-%v", rf.me, rf.currentTerm)
			select {
			//receive a vote request
			case <-rf.granted:
			//receive heartbeat
			case <-rf.heartbeat:
			//timeout
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+300)):
				Warning("Follower raft-%v:%v receive timeout and become candidate", rf.me, rf.currentTerm)
				rf.status = Candidate
			}

		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			Info("Raft-%v switch-candidate: term-%v", rf.me, rf.currentTerm)
			rf.votedFor = rf.me
			rf.persist()
			rf.voteCount = 1
			rf.mu.Unlock()
			rf.sendAllRequestVotes()

			select {
			//election timeout and try again
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(200)+300)):
				Error("raft-%v trying to candidate timeout", rf.me)
			//other become the leader and start to send heartbeat
			case <-rf.heartbeat:
				Warning("Candidate raft-%v:%v receive heartbeat and cancel candidate", rf.me, rf.currentTerm)
				rf.status = Follower
			//election success
			case <-rf.electWin:
				rf.mu.Lock()
				Success("Candidate raft-%v:%v become leader with vote:%v", rf.me, rf.currentTerm, rf.voteCount)
				rf.status = Leader
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) candidatesLogIsUp2Date(argsTerm int, argsIndex int) bool {
	rfLastTerm := rf.getLastLogTerm()
	rfLastIndex := rf.getLastLogIndex()

	if argsTerm != rfLastTerm {
		return argsTerm > rfLastTerm
	}
	return argsIndex >= rfLastIndex

}
