package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const MaxWaitTime = 600 * time.Millisecond

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	*GetArgs
	*PutAppendArgs
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied int

	kvMachine           KVMachine
	lastPutAppendId     map[int64]int64
	notifyChs_PutAppend map[int]chan PutAppendReply
	notifyChs_Get       map[int]chan GetReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("{KVServer-%d} receives Get %s\n", kv.me, args.Key)
	defer DPrintf("{KVServer-%d} finishes Get %s, the reply is %v\n", kv.me, args.Key, reply)
	logId, _, isLeader := kv.rf.Start(Op{GetArgs: args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// DPrintf("{KVServer-%d} try to get %s with logId: %d\n", kv.me, args.Key, logId)

	kv.mu.Lock()
	ch_get := kv.getNotifyCh_Get(logId)
	kv.mu.Unlock()

	select {
	case result := <-ch_get:
		reply.Err = result.Err
		reply.Value = result.Value
	case <-time.After(MaxWaitTime):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.notifyChs_Get, logId)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) getNotifyCh_Get(id int) chan GetReply {
	if _, ok := kv.notifyChs_Get[id]; !ok {
		kv.notifyChs_Get[id] = make(chan GetReply, 1)
	}
	return kv.notifyChs_Get[id]
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer DPrintf("{KVServer-%d} finishes %s {%s: %s}, the reply is %v\n", kv.me, args.Op, args.Key, args.Value, reply)
	kv.mu.RLock()
	if kv.isDuplicate(args.ClientId, args.RequestId) {
		kv.mu.RUnlock()
		reply.Err = OK
		return
	}
	kv.mu.RUnlock()

	logId, _, isLeader := kv.rf.Start(Op{PutAppendArgs: args})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("{KVServer-%d} try to %s {%s: %s} with logId: %d\n", kv.me, args.Op, args.Key, args.Value, logId)

	kv.mu.Lock()
	ch_putAppend := kv.getNotifyCh_PutAppend(logId)
	kv.mu.Unlock()

	select {
	case result := <-ch_putAppend:
		reply.Err = result.Err
	case <-time.After(MaxWaitTime):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.notifyChs_PutAppend, logId)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDuplicate(clientId int64, requestId int64) bool {
	lastRequestId, ok := kv.lastPutAppendId[clientId]
	if !ok {
		return false
	}
	return requestId <= lastRequestId
}

func (kv *KVServer) getNotifyCh_PutAppend(id int) chan PutAppendReply {
	if _, ok := kv.notifyChs_PutAppend[id]; !ok {
		kv.notifyChs_PutAppend[id] = make(chan PutAppendReply, 1)
	}
	return kv.notifyChs_PutAppend[id]
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					DPrintf("{KVServer-%d} reveives applied log{%v}", kv.me, msg)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				op := msg.Command.(Op)
				if op.GetArgs != nil {
					DPrintf("{KVServer-%d} apply get %v.", kv.me, op.GetArgs.Key)
					value, err := kv.kvMachine.Get(op.GetArgs.Key)
					reply := GetReply{Err: err, Value: value}

					if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == msg.CommandTerm {
						if ch, ok := kv.notifyChs_Get[msg.CommandIndex]; ok {
							ch <- reply
						}
					}
				} else if op.PutAppendArgs != nil {
					var reply PutAppendReply
					if kv.isDuplicate(op.PutAppendArgs.ClientId, op.PutAppendArgs.RequestId) {
						DPrintf("{KVServer-%d} receives duplicated request{%v}\n", kv.me, msg)
						reply.Err = OK
					} else {
						DPrintf("{KVServer-%d} apply %s {%s: %s}.\n", kv.me, op.PutAppendArgs.Op, op.PutAppendArgs.Key, op.PutAppendArgs.Value)
						if op.PutAppendArgs.Op == "Put" {
							reply.Err = kv.kvMachine.Put(op.PutAppendArgs.Key, op.PutAppendArgs.Value)
						} else if op.PutAppendArgs.Op == "Append" {
							reply.Err = kv.kvMachine.Append(op.PutAppendArgs.Key, op.PutAppendArgs.Value)
						}
						kv.lastPutAppendId[op.PutAppendArgs.ClientId] = op.PutAppendArgs.RequestId
					}

					if _, isLeader := kv.rf.GetState(); isLeader {
						if ch, ok := kv.notifyChs_PutAppend[msg.CommandIndex]; ok {
							ch <- reply
						}
					}
				} else {
					DPrintf("{KVServer-%d} receives unknown command{%v}", kv.me, msg)
				}

				if kv.isNeedSnapshot() {
					DPrintf("{KVServer-%d} needs snapshot\n", kv.me)
					kv.snapshot(msg.CommandIndex)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				kv.mu.Lock()
				kv.reloadBySnapshot(msg.Snapshot)
				kv.lastApplied = msg.CommandIndex
				kv.mu.Unlock()
			} else {
				DPrintf("{KVServer-%d} receives unknown msg{%v}", kv.me, msg)
			}
		}
	}
}

func (kv *KVServer) isNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return kv.rf.GetRaftStateSize() > kv.maxraftstate
}

func (kv *KVServer) snapshot(lastAppliedLogId int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if mr, lr := e.Encode(kv.kvMachine), e.Encode(kv.lastPutAppendId); mr != nil ||
		lr != nil {
		DPrintf("{KVServer-%d} snapshot failed. kvMachine length: %v, result: {%v}, lastPutAppendId: {%v}, result: {%v},",
			kv.me, len(kv.kvMachine.KV), mr, kv.lastPutAppendId, lr)
		return
	}

	data := w.Bytes()
	kv.rf.Snapshot(lastAppliedLogId, data)
	DPrintf("{KVServer-%d} snapshot succeeded\n", kv.me)
}

func (kv *KVServer) reloadBySnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var kvMachine KVMachine
	var lastPutAppendId map[int64]int64

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kvMachine) != nil ||
		d.Decode(&lastPutAppendId) != nil {
		DPrintf("{KVServer-%d} reloadBySnapshot failed\n", kv.me)
	}

	DPrintf("{KVServer-%d} reloadBySnapshot succeeded\n", kv.me)
	kv.lastPutAppendId = lastPutAppendId
	kv.kvMachine = kvMachine
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		me:                  me,
		applyCh:             applyCh,
		rf:                  raft.Make(servers, me, persister, applyCh),
		dead:                0,
		maxraftstate:        maxraftstate,
		lastApplied:         0,
		kvMachine:           *newKVMachine(),
		lastPutAppendId:     make(map[int64]int64),
		notifyChs_PutAppend: make(map[int]chan PutAppendReply),
		notifyChs_Get:       make(map[int]chan GetReply),
	}

	kv.reloadBySnapshot(persister.ReadSnapshot())
	go kv.applier()
	DPrintf("{KVServer-%d} started\n", me)

	return kv
}
