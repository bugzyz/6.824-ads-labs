package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) runServer() {
	for {
		switch rf.status {
		case Leader:
			//sending heartbeat to follower
			rf.sendAllAppendEntries()
			time.Sleep(time.Millisecond * 120)

		case Follower:
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
				rf.mu.Lock()
				rf.status = Leader
				rf.mu.Unlock()
			}
		}
	}
}

//to do: the goroutine function for checking whether rf's election win
func (rf *Raft) checkElectionWin() {

}
