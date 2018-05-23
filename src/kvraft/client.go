package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	//client id
	ClientId int64
	//operation num: to ignore duplicate operation return from different raft(leader)
	opNum int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.leader = 0
	ck.ClientId = nrand()
	ck.opNum = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	//init getArgs
	gArgs := new(GetArgs)
	gArgs.Key = key

	//use mod to reduce the time of trying who is leader
	for ; ; ck.leader = (ck.leader + 1) % (len(ck.servers)) {
		reply := new(GetReply)
		ok := ck.servers[ck.leader].Call("KVServer.Get", gArgs, reply)

		//find the true leader
		if ok && !reply.WrongLeader {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			} else {
				return ""
			}
		} else {
			return ""
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	paArgs := new(PutAppendArgs)
	paArgs.Key = key
	paArgs.Op = op
	paArgs.Value = value
	paArgs.ClientId = ck.ClientId

	//a new operation needs a incremental opNum
	ck.opNum++
	paArgs.OpNum = ck.opNum

	reply := new(PutAppendReply)

	for ; ; ck.leader = (ck.leader + 1) % len(ck.servers) {
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", paArgs, reply)

		if ok && !reply.WrongLeader {
			Success("client OP:%v success", op)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
