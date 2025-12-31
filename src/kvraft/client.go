package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int64
	clientId  int64
	requestId int64
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
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.requestId = 0
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf("{Clinetn-%d} try to get '%s'\n", ck.clientId, key)
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: ck.requestId}
	for {
		var reply GetReply
		if ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) && reply.Err != ErrWrongLeader {
			if reply.Err == OK {
				DPrintf("{Clinetn-%d} Get {'%v': '%v'}\n", ck.clientId, key, reply.Value)
				ck.requestId++
				return reply.Value
			} else if reply.Err == ErrNoKey {
				DPrintf("{Clinetn-%d} does not have key{'%v'}\n", ck.clientId, key)
				return ""
			} else {
				DPrintf("{Clinetn-%d} Get wrong reply{%v} for '%v'", ck.clientId, reply, key)
			}
		} else {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("{Clinetn-%d} try to %s {'%v': '%v'}\n", ck.clientId, op, key, value)
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, RequestId: ck.requestId}
	for {
		var reply PutAppendReply
		if ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) && reply.Err == OK {
			DPrintf("{Clinetn-%d} %s {'%v': '%v'} success\n", ck.clientId, op, key, value)
			ck.requestId++
			break
		} else {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
