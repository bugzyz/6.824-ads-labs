package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const printIF = 1

//my func
func (rf *Raft) printInfo() {
	if printIF > 0 {
		statusNum := rf.status
		var status string
		switch statusNum {
		case Follower:
			status = "Follower"
		case Candidate:
			status = "Candidate"
		case Leader:
			status = "Leader"
		}
		log.Printf("==printInfo: \t\t%v %v", rf.me, status)
	}
	return
}
