package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"kvraft/src/kvraft1/rsm"
	"kvraft/src/kvsrv1/rpc"
	"kvraft/src/labgob"
	"kvraft/src/labrpc"
	"kvraft/src/tester1"
)

type KVServer struct {
	me              int   //id of the server.
	dead            int32 // set by Kill()
	rsm             *rsm.RSM
	mu              sync.Mutex
	KeyValueStore   map[string]ValueTuple
	DuplicatedCache map[UniqueIdentifier]any // map the (clientid, requestid) to the result. so we can return from
	// this itself.
}

type UniqueIdentifier struct {
	ClientId  int64
	RequestId int64
}

type ValueTuple struct {
	Val     string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	//this is where the updation and returning of data must happen.
	//log.Printf("req type is: %T", req) this just returns rsm.op type.
	//log.Printf("req type is: %T", req)
	switch args := req.(type) {
	case *rpc.GetArgs:
		//log.Printf("get args is %v", args)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		valueTuple, exists := kv.KeyValueStore[args.Key]
		var reply rpc.GetReply
		if !exists {
			log.Printf("Does not exist in the map GET")
			reply.Err = rpc.ErrNoKey
		} else {
			reply.Err = rpc.OK
			reply.Value = valueTuple.Val
			reply.Version = valueTuple.Version
		}
		return reply
	case *rpc.PutArgs:
		//log.Printf("put args is %v", args)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		valueTuple, exists := kv.KeyValueStore[args.Key]
		var reply rpc.PutReply
		if !exists {
			if args.Version == 0 {
				kv.KeyValueStore[args.Key] = ValueTuple{args.Value, args.Version}
				reply.Err = rpc.OK
			} else {
				log.Printf("Does not exist in the map PUT and its arg version is not 0")
				reply.Err = rpc.ErrNoKey
			}
		} else {
			//check for version mismatch.
			if args.Version != valueTuple.Version {
				reply.Err = rpc.ErrVersion
			} else {
				kv.KeyValueStore[args.Key] = ValueTuple{args.Value, args.Version + 1} //increment version number
				reply.Err = rpc.OK
			}
		}
		return reply
	default:
		//log.Printf("Unknown command %v", req)
		//log.Printf("args type is: %T", args)
		return nil
	}

}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	// first send to rsm.submit, cannot serve read directly from our local store as this could break if the current
	//node was partitioned and had no idea about other nodes.
	err, res := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	out := res.(rpc.GetReply)
	//now check in our localstore for the values, version and all that.
	reply.Value = out.Value
	reply.Version = out.Version
	reply.Err = out.Err

	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	//kv.DuplicatedCache[UniqueIdentifier{ClientId: args.ClientId, RequestId: args.RequestId}] = out
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	//first gotta check what the majority consensus is, only then can we return or modify values.
	err, res := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	out := res.(rpc.PutReply)
	reply.Err = out.Err
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	//kv.DuplicatedCache[UniqueIdentifier{ClientId: args.ClientId, RequestId: args.RequestId}] = out

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
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(GetClientArgs{})
	labgob.Register(PutClientArgs{})
	labgob.Register(GetClientReply{})
	labgob.Register(PutClientReply{})
	kv := &KVServer{me: me,
		KeyValueStore:   make(map[string]ValueTuple),
		DuplicatedCache: make(map[UniqueIdentifier]any)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
