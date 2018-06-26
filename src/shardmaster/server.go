package shardmaster

import (
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	//my code
	//the channel to send the request finished message
	result map[int]chan Op
	//use the clientId as the key, and the opNum as the value
	//As the opNum of a specific clientId is monotonically increasing, it will be easy to detect the duplicate request from the same clientId
	detectDup map[int64]int
}

type Op struct {
	// Your data here.
	//operation type: join,leave,move,query
	Type string

	//void *
	interface{} configInfo

	//the id of which the request comes from
	ClientId int64

	//the num of the request which used to detect duplicate request return from different raft
	OpNum int
}

//when the new operation(join,leave...) arrives, use the callStart to start a raft replicating
func (master *ShardMaster) callStart(op Op) bool {
	index, _, isLeader := master.rf.Start(op)

	//the raft is no longer the leader -> return failed and the client will try it again
	if isLeader == false {
		return false
	}
	Success("master-%v now connect the true leader", master.me)
	master.mu.Lock()
	ch, ok := master.result[index]

	//if there isn't a channel used to pass the finish message than create one
	if !ok {
		ch = make(chan Op, 1)
		master.result[index] = ch
	}

	master.mu.Unlock()
	select {
	case cmd := <-ch:
		if master.me == 0 {
			Trace("master-%v receiving a cmd-%v and the cmd==op is %v", master.me, cmd, cmd == op)
		}
		return cmd == op
	case <-time.After(800 * time.Millisecond):
		return false
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Type: "join"}

	//call raft to replicate
	ok := sm.callStart(op)

	// callStart() failed the leader is changed and return the wrongleader reply
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	sm.mu.Lock()

	newConfig := Config{}
	sm.configs = Append(sm.configs, s)

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.detectDup = make(map[int64]int)
	sm.result = make(map[int]chan Op)

	go sm.receiveApplyMsgAndApply()

	return sm
}

func (sm *ShardMaster) receiveApplyMsgAndApply() {
	for {
		//receive the command(join,leave...) from raft cluster
		msg := <-sm.applyCh
		sm.mu.Lock()

		op := msg.Command.(Op)
		//if opNum is not a duplicate opNum then execute it
		if opNum, ok := sm.detectDup[op.ClientId]; !ok || op.OpNum > opNum {
			switch op.Type {
			//todo: do something based on the op sm receive
			case "join":
			case "move":
			case "leave":
			case "query":
			}
			sm.detectDup[op.ClientId] = op.OpNum
		}

		ch, ok := sm.result[msg.CommandIndex]
		sm.mu.Unlock()

		if ok {
			ch <- op
		}

	}
}
