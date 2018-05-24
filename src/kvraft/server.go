package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	//operation type
	Type  string
	Key   string
	Value string
	//the id of which the request comes from
	ClientId int64
	//the num of the request which used to detect duplicate request return from different raft
	OpNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//storage
	storage map[string]string
	//the channel to send the request finished message
	result map[int]chan Op
	//use the clientId as the key, and the opNum as the value
	//As the opNum of a specific clientId is monotonically increasing, it will be easy to detect the duplicate request from the same clientId
	detectDup map[int64]int
}

func (kv *KVServer) callStart(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)

	//the raft is no longer the leader -> return failed and the client will try it again
	if isLeader == false {
		return false
	}
	Success("kv-%v now connect the true leader", kv.me)
	kv.mu.Lock()
	ch, ok := kv.result[index]

	//if there isn't a channel used to pass the finish message than create one
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}

	kv.mu.Unlock()
	select {
	case cmd := <-ch:
		Trace("kv-%v receiving a cmd-%v and the cmd==op is %v", kv.me, cmd, cmd == op)
		return cmd == op
	case <-time.After(800 * time.Millisecond):
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//init operation
	op := Op{Type: "GET", Key: args.Key}

	ok := kv.callStart(op)

	// callStart() failed the leader is changed and return the wrongleader reply
	if !ok {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	kv.mu.Lock()
	value, exist := kv.storage[args.Key]
	kv.mu.Unlock()

	if exist {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, OpNum: args.OpNum}

	ok := kv.callStart(op)

	if !ok {
		reply.WrongLeader = true
		Error("kvserver-%v callStart() failed! and return reply:%v", kv.me, reply)
		return
	}

	reply.WrongLeader = false
	reply.Err = OK

	Success("kvserver-%v callStart() success! and return reply:%v", kv.me, reply)
}

//after receiving a committed operation than apply it on the kv.storage
func (kv *KVServer) executeOpOnKvServer(op Op) {
	switch op.Type {
	case "Put":
		kv.storage[op.Key] = op.Value
	case "Append":
		kv.storage[op.Key] += op.Value
	default:
		Error("kvServer-%v executeOpOnKvServer func went wrong", kv.me)
	}
	Trace1("KvServer-%v now has the storage of %v", kv.me, kv.storage)
}

//this func is a for loop that make that kv-server keeps receiving new committed op from the associated raft agreement
//and apply the op to the storage
func (kv *KVServer) receiveApplyMsgAndApply() {
	for {
		//get the op that commit by those rafts
		msg := <-kv.applyCh
		//convert the command interface{} to Op
		op := msg.Command.(Op)

		//debug
		Error("kv-%v receiving a msg:%v", kv.me, msg)

		kv.mu.Lock()

		if op.Type != "GET" {
			//record the opNum of every clientId so that if the op.Opnum <= opNum, it means that this operation is executed before
			if opNum, ok := kv.detectDup[op.ClientId]; !ok || op.OpNum > opNum {
				kv.executeOpOnKvServer(op)
				// Trace("kv-%v receiving a op.OpNum:%v > opNum:%v from clientId:%v", kv.me, op.OpNum, opNum, op.ClientId)
				kv.detectDup[op.ClientId] = op.OpNum
			}
		}

		ch, ok := kv.result[msg.CommandIndex]

		if ok {
			//tell the RPC handler of kvserver that the agreement is done and the op is applied on storage
			ch <- op
			// Error("kv-%v sending a op:%v to ch", kv.me, op)
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.result = make(map[int]chan Op)
	kv.detectDup = make(map[int64]int)

	go kv.receiveApplyMsgAndApply()
	return kv
}
