package shardkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

type DataBase map[string]string
type Duplicate map[int64]*LatestReply

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string // "Get", "Put" or "Append"
	ClientID int64  // client id
	SeqNo    int    // request sequence number
}

// new config to switch
type Cfg struct {
	Config shardmaster.Config
}

// migrate data and dup table to spread
type Mig struct {
	Num   int
	Shard int
	Gid   int
	Data  DataBase
	Dup   Duplicate
}

type CleanUp struct {
	Num   int
	Shard int
	Gid   int
}

type LatestReply struct {
	Seq   int      // latest request
	Reply GetReply // latest reply
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck           *shardmaster.Clerk   // talk to master
	persist       *raft.Persister      // store snapshot
	shutdownCh    chan struct{}        // shutdown gracefully
	db            DataBase             // data store
	notifyChs     map[int]chan Err     // per log entry
	duplicate     Duplicate            // duplication detection table
	snapshotIndex int                  // snapshot
	configs       []shardmaster.Config // configs[0] is current configuration
	workList      map[int]MigrateWork  // config No. -> work to be done
	gcHistory     map[int]int          // shard->config
}

type MigrateWork struct {
	RecFrom []Item
	Last    shardmaster.Config
}
type Item struct {
	Shard, Gid int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	defer func() {
		Trace("[%d-%d]: Get args: %v, reply: %v. (shard: %d)\n", kv.gid, kv.me, args, reply, key2shard(args.Key))
	}()

	// Your code here.
	// if the kv's raft no more the leader then return
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	shard := key2shard(args.Key)
	// the key is not responsible for this kv
	kv.mu.Lock()
	if kv.configs[0].Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	// if responsible, check whether already receive data from previous owner
	// if the worklist have a non-empty recFrom which means the migratin work incompleted. So let the client try next time even though the key2shard pointing to this kv server
	cur := kv.configs[0].Num
	if work, ok := kv.workList[cur]; ok {
		recFrom := work.RecFrom
		for _, item := range recFrom {
			// still waiting data? postpone client request
			if shard == item.Shard {
				kv.mu.Unlock()
				reply.Err = ErrWrongGroup
				Error("[%d-%d]: Get rpc: postpone client request, still waiting data.\n", kv.gid, kv.me)
				return
			}
		}
	}

	// duplicate put/append request
	// if the request is duplicate then return the reply been replied before
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		// filter duplicate
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = dup.Reply.Value
			Trace("[%d-%d]: Get rpc: duplicate request: %d - %d.\n", kv.gid, kv.me, args.SeqNo, dup.Seq)
			return
		}
	}

	cmd := Op{Key: args.Key, Op: "Get", ClientID: args.ClientID, SeqNo: args.SeqNo}
	index, term, _ := kv.rf.Start(cmd)

	ch := make(chan Err)
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	// wait for Raft to complete agreement
	select {
	case err := <-ch:
		// lose leadership
		curTerm, isLeader := kv.rf.GetState()
		// what if still leader, but different term? let client retry
		if !isLeader || term != curTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}

		reply.Err = err
		if err == OK {
			kv.mu.Lock()
			if value, ok := kv.db[args.Key]; ok {
				reply.Value = value
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		}
	case <-kv.shutdownCh:
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer func() {
		Trace("[%d-%d]: PutAppend args: %v, reply: %v. (shard: %d)\n", kv.gid, kv.me, args, reply,
			key2shard(args.Key))
	}()

	// Your code here.
	// not the leader now
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	shard := key2shard(args.Key)
	// not responsible for key?
	kv.mu.Lock()
	if kv.configs[0].Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	// if responsible, check whether already receive data from previous owner
	cur := kv.configs[0].Num
	if work, ok := kv.workList[cur]; ok {
		recFrom := work.RecFrom
		for _, item := range recFrom {
			// still waiting data?
			if shard == item.Shard {
				kv.mu.Unlock()
				// postpone client
				Error("[%d-%d]: PutAppend rpc: postpone client request, still waiting data.\n", kv.gid, kv.me)
				reply.Err = ErrWrongGroup
				return
			}
		}
	}
	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		// filter duplicate
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			Trace("[%d-%d]: PutAppend rpc: duplicate request: %d - %d.\n", kv.gid, kv.me, args.SeqNo, dup.Seq)
			return
		}
	}

	// new request
	cmd := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientID: args.ClientID, SeqNo: args.SeqNo}
	index, term, _ := kv.rf.Start(cmd)
	ch := make(chan Err)
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	// wait for Raft to complete agreement
	select {
	case err := <-ch:
		// lose leadership
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != curTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}
		reply.Err = err
	case <-kv.shutdownCh:
		return
	}
}

// Migrate Configuration
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	defer func() {
		Trace1("[%d-%d]: request Migrate, args: %v, reply: %t, %q, db: %v.\n", kv.gid, kv.me, args,
			reply.WrongLeader, reply.Err, reply.Data)
	}()

	// not leader?
	// every servers in the same group have their own raft group and the servers in the same group have the same data which will be delivered by the leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if receive unexpected newer config migrate? postpone until itself detect new config
	if args.Num > kv.configs[0].Num {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	//packing its data to the shardkv who connects the raft leader of its group and asks for the data migration
	reply.Data, reply.Dup = kv.copyDataDup(args.Shard)

	reply.WrongLeader = false
	reply.Err = OK
	reply.Shard = args.Shard
	reply.Num = args.Num
	reply.Gid = kv.gid

}

// GC RPC: called by other group to notify cleaning up unnecessary Shards
func (kv *ShardKV) CleanUp(args *CleanUpArgs, reply *CleanUpReply) {
	defer func() {
		Info1("[%d-%d]: request CleanUp, args: %v, reply: %v.\n", kv.gid, kv.me, args, reply)
	}()

	// not leader?
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if receive unexpected newer config cleanup? postpone until itself detect new config
	if args.Num > kv.configs[0].Num {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	if kv.gcHistory[args.Shard] < args.Num {
		kv.rf.Start(CleanUp{Num: args.Num, Shard: args.Shard, Gid: args.Gid})
	}

	reply.WrongLeader = false
	reply.Err = OK
}

// should be called when holding the lock
func (kv *ShardKV) applyMigratedData(mig Mig) {
	defer func() {
		Trace1("[%d-%d]: kv.workList: %v\n", kv.gid, kv.me, kv.workList)
	}()

	// update data
	for k, v := range mig.Data {
		if key2shard(k) == mig.Shard {
			kv.db[k] = v
		}
	}
	// update duplicate table
	for client, dup := range mig.Dup {
		d, ok := kv.duplicate[client]
		if ok {
			if d.Seq < dup.Seq {
				kv.duplicate[client].Seq = dup.Seq
				kv.duplicate[client].Reply = dup.Reply
			}
		} else {
			kv.duplicate[client] = dup
		}
	}
	// update work list
	if work, ok := kv.workList[mig.Num]; ok {
		recFrom := work.RecFrom
		var done = -1
		for i, item := range recFrom {
			if item.Shard == mig.Shard && item.Gid == mig.Gid {
				done = i

				// if leader, it's time to notify original owner to cleanup
				if _, isLeader := kv.rf.GetState(); isLeader {
					go kv.requestCleanUp(item.Shard, item.Gid, &work.Last)
				}
				break
			}
		}
		if done != -1 {
			tmp := recFrom[done+1:]
			recFrom = recFrom[:done]
			recFrom = append(recFrom, tmp...)

			// done
			if len(recFrom) == 0 {
				delete(kv.workList, mig.Num)
				return
			}
			// update
			kv.workList[mig.Num] = MigrateWork{recFrom, kv.workList[mig.Num].Last}
		}
	}
}

// garbage collection of state when lost ownership
// should be called when holding the lock
func (kv *ShardKV) shardGC(args CleanUp) {
	for k := range kv.db {
		if shard := key2shard(k); shard == args.Shard {
			delete(kv.db, k)
		}
	}
	Success1("[%d-%d]: server %d has gc shard: %d @ config: %d, from gid: %d\n",
		kv.gid, kv.me, kv.me, args.Shard, args.Num, args.Gid)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdownCh)
}

// applyDaemon receive applyMsg from Raft layer, apply to Key-Value state machine
// then notify related client if it's leader
func (kv *ShardKV) applyDaemon() {
	for {
		select {
		case msg, ok := <-kv.applyCh:
			if ok {
				// have snapshot to apply?
				if msg.UseSnapshot {
					kv.mu.Lock()
					kv.readSnapshot(msg.Snapshot)
					// must be persisted, in case of crashing before generating another snapshot
					kv.generateSnapshot(msg.CommandIndex)
					kv.mu.Unlock()
					continue
				}
				// have client's request? must filter duplicate command
				if msg.Command != nil && msg.CommandIndex > kv.snapshotIndex {
					var err Err = OK
					kv.mu.Lock()
					switch cmd := msg.Command.(type) {
					case Op:
						// switch to new config already?
						shard := key2shard(cmd.Key)
						if kv.configs[0].Shards[shard] != kv.gid {
							Success1("[%d-%d]: server %d (gid: %d) has switched to new config %d, "+
								"no responsibility for shard %d\n",
								kv.gid, kv.me, kv.me, kv.gid, kv.configs[0].Num, shard)
							err = ErrWrongGroup
							break
						}
						if dup, ok := kv.duplicate[cmd.ClientID]; !ok || dup.Seq < cmd.SeqNo {
							switch cmd.Op {
							case "Get":
								kv.duplicate[cmd.ClientID] = &LatestReply{Seq: cmd.SeqNo,
									Reply: GetReply{Value: kv.db[cmd.Key]}}
							case "Put":
								kv.db[cmd.Key] = cmd.Value
								kv.duplicate[cmd.ClientID] = &LatestReply{Seq: cmd.SeqNo}
							case "Append":
								kv.db[cmd.Key] += cmd.Value
								kv.duplicate[cmd.ClientID] = &LatestReply{Seq: cmd.SeqNo}
							default:
								Error1("[%d-%d]: server %d receive invalid cmd: %v\n", kv.gid, kv.me, kv.me, cmd)
								panic("invalid command operation")
							}
						}
					case Cfg:
						// duplicate detection: newer than current config
						if cmd.Config.Num > kv.configs[0].Num {
							kv.switchConfig(&cmd.Config)
						}
					case Mig:
						// apply data and dup, then start to accept client requests
						if cmd.Num == kv.configs[0].Num && !kv.isMigrateDone("applyMigrateData") {
							kv.applyMigratedData(cmd)
						}
					case CleanUp:
						if kv.gcHistory[cmd.Shard] < cmd.Num && cmd.Num <= kv.configs[0].Num {
							if kv.configs[0].Shards[cmd.Shard] != kv.gid {
								Error3("migration done...executing cleanup")
								kv.shardGC(cmd)
								kv.gcHistory[cmd.Shard] = cmd.Num
							} else {
								kv.gcHistory[cmd.Shard] = kv.configs[0].Num
							}
						} else {
							Success1("[%d-%d]: server %d, shard: %d, config: %d - %d, gc history: %d\n",
								kv.gid, kv.me, kv.me, cmd.Shard, cmd.Num, kv.configs[0].Num, kv.gcHistory[cmd.Shard])
						}
					default:
						panic("Oops... unknown cmd type from applyCh")
					}
					// snapshot detection: up through msg.Index
					if needSnapshot(kv) {
						// save snapshot and notify raft
						kv.generateSnapshot(msg.CommandIndex)
						kv.rf.NewSnapShot(msg.CommandIndex)
					}
					// notify channel
					if notifyCh, ok := kv.notifyChs[msg.CommandIndex]; ok && notifyCh != nil {
						notifyCh <- err
						delete(kv.notifyChs, msg.CommandIndex)
					}
					kv.mu.Unlock()
				}
			}
		case <-kv.shutdownCh:
			Error("[%d-%d]: server %d is shutting down.\n", kv.gid, kv.me, kv.me)
			return
		}
	}
}

func needSnapshot(kv *ShardKV) bool {
	if kv.maxraftstate < 0 {
		return false
	}
	if kv.maxraftstate < kv.persist.RaftStateSize() {
		return true
	}
	// abs < 10% of max
	var abs = kv.maxraftstate - kv.persist.RaftStateSize()
	var threshold = kv.maxraftstate / 10
	if abs < threshold {
		return true
	}
	return false
}

// which index?
func (kv *ShardKV) generateSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.snapshotIndex = index

	e.Encode(kv.db)
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.duplicate)
	e.Encode(kv.configs)
	e.Encode(kv.workList)
	e.Encode(kv.gcHistory)

	data := w.Bytes()
	kv.persist.SaveSnapshot(data)

	Success2("[%d-%d]: server %d generate snapshot (configs: %v, worklist: %v).\n",
		kv.gid, kv.me, kv.me, kv.configs, kv.workList)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	kv.db = make(DataBase)
	kv.duplicate = make(Duplicate)

	d.Decode(&kv.db)
	d.Decode(&kv.snapshotIndex)
	d.Decode(&kv.duplicate)
	d.Decode(&kv.configs)
	d.Decode(&kv.workList)
	d.Decode(&kv.gcHistory)

	Trace2("[%d-%d]: server %d read snapshot (configs: %v, worklist: %v).\n",
		kv.gid, kv.me, kv.me, kv.configs, kv.workList)

	// if snapshot occurs in middle of migration
	if kv.configs[0].Num != 0 {
		go kv.restartMigration()
	}
}

// should be called when holding the lock
// all server in a replica group switch to new config at the same point
func (kv *ShardKV) switchConfig(new *shardmaster.Config) {
	var old = &kv.configs[0]
	kv.generateWorkList(old, new)

	Trace1("[%d-%d]: server %d switch to new config (%d->%d, shards: %v, workList: %v, configs: %v).\n",
		kv.gid, kv.me, kv.me, kv.configs[0].Num, new.Num, new.Shards, kv.workList[new.Num], len(kv.configs))

	if len(kv.configs) > 2 {
		log.Println(new.Num, kv.configs)
		panic("len(kv.configs) > 2")
	}

	// for all server: switch to new config
	if len(kv.configs) == 1 {
		kv.configs[0] = *new
	} else {
		if kv.configs[1].Num != new.Num {
			log.Println(new.Num, kv.configs)
			panic("kv.configs[1].Num != new.Num")
		}
		kv.configs = kv.configs[1:]
	}

	// for leader
	if _, isLeader := kv.rf.GetState(); isLeader {
		if work, ok := kv.workList[new.Num]; ok && len(work.RecFrom) > 0 {
			// safe point to copy data and dup
			go kv.requestShards(old, new.Num)
		}
		return
	}
}

// should be called when holding the lock
func (kv *ShardKV) copyDataDup(shard int) (DataBase, Duplicate) {
	data := make(DataBase)
	dup := make(Duplicate)
	for k, v := range kv.db {
		if key2shard(k) == shard {
			data[k] = v
		}
	}
	for k, v := range kv.duplicate {
		reply := &LatestReply{v.Seq, v.Reply}
		dup[k] = reply
	}
	return data, dup
}

// using new config's Num.
func (kv *ShardKV) requestShards(old *shardmaster.Config, num int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	recFrom := kv.workList[num].RecFrom

	fillMigrateArgs := func(shard int) *MigrateArgs {
		return &MigrateArgs{
			Num:   num,
			Shard: shard,
			Gid:   kv.gid,
		}
	}
	replyHandler := func(reply *MigrateReply) {
		// still leader?
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}
		kv.mu.Lock()
		defer kv.mu.Unlock()

		// still at current config Num and migrate not done? if not, ignore reply data
		if reply.Num == kv.configs[0].Num && !kv.isMigrateDone("replyHandler") {
			var mig = Mig{Num: reply.Num, Shard: reply.Shard, Gid: reply.Gid, Data: reply.Data, Dup: reply.Dup}
			kv.rf.Start(mig)

			Success1("[%d-%d]: leader %d receive shard: %d, from: %d).\n", kv.gid, kv.me, kv.me,
				reply.Shard, reply.Gid)
		}
	}
	// act as a normal client
	for _, task := range recFrom {
		go func(s, g int) {
			args := fillMigrateArgs(s)
			for {
				// shutdown?
				select {
				case <-kv.shutdownCh:
					return
				default:
				}
				// still leader?
				if _, isLeader := kv.rf.GetState(); !isLeader {
					return
				}
				if servers, ok := old.Groups[g]; ok {
					// try each server for the shard.
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])

						var reply MigrateReply
						ok := srv.Call("ShardKV.Migrate", args, &reply)
						if ok && reply.WrongLeader == false && reply.Err == OK {
							Success3("successfully call 1 migrate server:%v in shard:%v gid:%v now", si, s, g)
							replyHandler(&reply)
							return
						}
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(task.Shard, task.Gid)
	}
}

// notify cleanup garbage shards
func (kv *ShardKV) requestCleanUp(shard, gid int, config *shardmaster.Config) {
	args := &CleanUpArgs{
		Num:   kv.configs[0].Num, // config version
		Shard: shard,
		Gid:   kv.gid,
	}

	Info1("[%d-%d]: leader %d issue cleanup shard: %d, gid: %d).\n", kv.gid, kv.me, kv.me, shard, gid)

	for {
		// shutdown?
		select {
		case <-kv.shutdownCh:
			return
		default:
		}
		// still leader?
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}
		if servers, ok := config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])

				var reply CleanUpReply
				ok := srv.Call("ShardKV.CleanUp", args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// should be called when holding the lock
func (kv *ShardKV) generateWorkList(old, new *shardmaster.Config) {
	// 0 is initial state, no need to send any shards
	if old.Num == 0 {
		return
	}
	var os, ns = make(map[int]bool), make(map[int]bool)
	for s, g := range old.Shards {
		if g == kv.gid {
			os[s] = true
		}
	}
	for s, g := range new.Shards {
		if g == kv.gid {
			ns[s] = true
		}
	}
	var recFrom []Item
	for k, _ := range ns {
		if !os[k] {
			recFrom = append(recFrom, Item{k, old.Shards[k]})
		}
	}
	if len(recFrom) != 0 {
		kv.workList[new.Num] = MigrateWork{recFrom, *old}
	}
}

// restart migration after crash, if necessary
func (kv *ShardKV) restartMigration() {
	kv.mu.Lock()
	cur := kv.configs[0].Num
	work, ok := kv.workList[cur]
	kv.mu.Unlock()

	// clear, no unfinished migration
	if cur == 0 || !ok {
		return
	}

	// unfortunately, in middle of config migration when generates snapshot and then crash
	for {
		select {
		case <-kv.shutdownCh:
			return
		default:
			kv.mu.Lock()
			now := kv.configs[0].Num
			kv.mu.Unlock()

			if cur != now {
				Success1("done....\n")
				return
			}

			if _, isLeader := kv.rf.GetState(); isLeader {
				go kv.requestShards(&work.Last, now)
			}

			time.Sleep(100 * time.Millisecond)
			Success1("[%d-%d]: restartMigration: config: %d, work: %v\n", kv.gid, kv.me, now, work.RecFrom)
		}
	}
}

// get next config: query shard master per 100ms
func (kv *ShardKV) getNextConfig() {
	for {
		select {
		case <-kv.shutdownCh:
			return
		default:
			kv.mu.Lock()
			cur := kv.configs[0].Num
			kv.mu.Unlock()

			// try to get next config
			config := kv.mck.Query(cur + 1)

			// get new config?
			kv.mu.Lock()
			if config.Num == kv.configs[0].Num+1 {
				// at most have two configs, old and new
				if len(kv.configs) == 1 {
					kv.configs = append(kv.configs, config)
				}

				// need waiting? just try, if failed, another leader will reply
				if _, isLeader := kv.rf.GetState(); isLeader && kv.isMigrateDone("getNextConfig()") {
					kv.rf.Start(Cfg{Config: kv.configs[1]})
					Success1("[%d-%d]: leader %d detect new config (%d->%d).\n", kv.gid, kv.me, kv.me,
						kv.configs[0].Num, kv.configs[1].Num)
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// should be called when holding the lock
func (kv *ShardKV) isMigrateDone(pos string) bool {
	_, ok := kv.workList[kv.configs[0].Num]
	defer func() {
		Success1("[%d-%d]: %s: Is migrate to %d Done? %t\n",
			kv.gid, kv.me, pos, kv.configs[0].Num, !ok)
	}()
	return !ok
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, gid int, masters []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(Cfg{})
	gob.Register(Mig{})
	gob.Register(CleanUp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persist = persister
	kv.shutdownCh = make(chan struct{})
	kv.db = make(DataBase)
	kv.notifyChs = make(map[int]chan Err)
	kv.duplicate = make(Duplicate)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.configs = []shardmaster.Config{{}}
	kv.workList = make(map[int]MigrateWork)
	kv.gcHistory = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// read snapshot when start
	kv.readSnapshot(kv.persist.ReadSnapshot())

	// long-running work
	go kv.applyDaemon()   // get log entry from raft layer
	go kv.getNextConfig() // query shard master for configuration

	Success("StartServer: %d-%d\n", kv.gid, kv.me)
	return kv
}
