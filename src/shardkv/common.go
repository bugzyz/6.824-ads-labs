package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time rafts.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64 // for duplicate request detection
	SeqNo    int   // sequence no
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64 // for duplicate request detection
	SeqNo    int   // sequence no
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type MigrateArgs struct {
	Num   int // sequence no
	Shard int
	Gid   int
}

type MigrateReply struct {
	WrongLeader bool
	Err         Err
	Num         int
	Shard       int
	Gid         int
	Data        map[string]string
	Dup         map[int64]*LatestReply
}

// garbage collection
type CleanUpArgs struct {
	Num   int
	Shard int
	Gid   int
}

type CleanUpReply struct {
	WrongLeader bool
	Err         Err
}
