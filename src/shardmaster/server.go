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

	//the id of which the request comes from
	ClientId int64

	//the num of the request which used to detect duplicate request return from different raft
	OpNum int

	//args
	Servers map[int][]string // args of "Join"
	GIDs    []int            // args of "Leave"
	Shard   int              // args of "Move"
	GID     int              // args of "Move"
	Num     int              // args of "Query" desired config number
}

//when the new operation(join,leave...) arrives, use the callStart to start a raft replicating
func (master *ShardMaster) callStart(op Op) bool {
	index, _, isLeader := master.rf.Start(op)

	//the raft is no longer the leader -> return failed and the client will try it again
	if isLeader == false {
		return false
	}
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
		return compareEqual(cmd, op)
	case <-time.After(800 * time.Millisecond):
		return false
	}
}

//to compare between 2 Op, check their clientId and opNum
func compareEqual(op1 Op, op2 Op) bool {
	if op1.ClientId == op2.ClientId && op1.OpNum == op2.OpNum {
		return true
	}
	return false
}

// should only be called when holding the lock
//if index == -1 or greater than the len of configs --> return the greatest config
//otherwise, return the exact config
func (sm *ShardMaster) getConfig(index int, config *Config) {
	if index == -1 || index >= len(sm.configs) {
		index = len(sm.configs) - 1
	}
	config.Num = sm.configs[index].Num
	config.Shards = sm.configs[index].Shards
	config.Groups = make(map[int][]string)
	//copy the configs[index].Group which is a map<string,string[]> to config
	for k, v := range sm.configs[index].Groups {
		var servers = make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Type: "join", ClientId: args.Info.ClientId, OpNum: args.Info.OpNum, Servers: args.Servers}

	//call raft to replicate
	ok := sm.callStart(op)

	// callStart() failed the leader is changed and return the wrongleader reply
	if !ok {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}
	reply.WrongLeader = false
	reply.Err = OK

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Type: "leave", ClientId: args.Info.ClientId, OpNum: args.Info.OpNum, GIDs: args.GIDs}

	//call raft to replicate
	ok := sm.callStart(op)

	// callStart() failed the leader is changed and return the wrongleader reply
	if !ok {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Type: "move", ClientId: args.Info.ClientId, OpNum: args.Info.OpNum, Shard: args.Shard, GID: args.GID}

	//call raft to replicate
	ok := sm.callStart(op)

	// callStart() failed the leader is changed and return the wrongleader reply
	if !ok {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Type: "query", ClientId: args.Info.ClientId, OpNum: args.Info.OpNum, Num: args.Num}

	//call raft to replicate
	ok := sm.callStart(op)

	// callStart() failed the leader is changed and return the wrongleader reply
	if !ok {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}
	reply.WrongLeader = false
	reply.Err = OK
	sm.mu.Lock()
	sm.getConfig(args.Num, &reply.Config)
	sm.mu.Unlock()
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

	Trace2("shardmaster-%v have peers:%v", sm.me, servers)
	go sm.receiveApplyMsgAndApply()

	return sm
}

func (sm *ShardMaster) rebalance(cnfg *Config) {

	if len(cnfg.Groups) == 0 {
		return
	}

	groupFamily := make([]int, 0)
	for gid, _ := range cnfg.Groups {
		groupFamily = append(groupFamily, gid)
	}

	for i, _ := range cnfg.Shards {
		cnfg.Shards[i] = groupFamily[i%len(groupFamily)]
	}

}

//execute the Join operation on shardmaster
func (sm *ShardMaster) execJoin(groups map[int][]string) {
	//step1. construct a new config{}
	config := Config{}
	//get the up-2-date config info
	sm.getConfig(-1, &config)
	//add the index num of the current config
	config.Num++
	for k, v := range groups {
		var servers = make([]string, len(v))
		copy(servers, v)
		config.Groups[k] = servers
	}

	//step2. rebalance
	sm.rebalance(&config)
	//step3.append new config to sm.configs
	sm.configs = append(sm.configs, config)

}

//execute the Leave operation on shardmaster
func (sm *ShardMaster) execLeave(GIDs []int) {
	//step1. construct a new config{}
	config := Config{}
	//get the up-2-date config info
	sm.getConfig(-1, &config)
	//add the index num of the current config
	config.Num++

	//for loop deletes the key==GIDs[i] in config.Groups
	for _, key := range GIDs {
		delete(config.Groups, key)
	}

	//step2. rebalance
	sm.rebalance(&config)
	//step3.append new config to sm.configs
	sm.configs = append(sm.configs, config)

}

//execute the Move operation on shardmaster
func (sm *ShardMaster) execMove(shardNum int, GID int) {
	//step1. construct a new config{}
	config := Config{}
	//get the up-2-date config info
	sm.getConfig(-1, &config)
	//add the index num of the current config
	config.Num++

	config.Shards[shardNum] = GID

	//stepNo there is not a rebalance in move
	//step3.append new config to sm.configs
	sm.configs = append(sm.configs, config)

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
			case "join":
				sm.execJoin(op.Servers)
			case "move":
				sm.execMove(op.Shard, op.GID)
			case "leave":
				sm.execLeave(op.GIDs)
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
