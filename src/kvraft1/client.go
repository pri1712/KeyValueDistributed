package kvraft

import (
	"kvraft/src/kvsrv1/rpc"
	"kvraft/src/kvtest1"
	"kvraft/src/tester1"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Clerk struct {
	Clnt       *tester.Clnt
	Servers    []string
	mu         sync.Mutex
	RequestId  int64 //to uniquely identify each request.
	ClientId   int64 //to uniquely identify each client at the server.
	LastLeader int
	// You will have to modify this struct.
}

type GetClientArgs struct {
	Key         string
	ClientId    int64
	RequestId   int64
	CommandType int //0 for get 1 for put
}

type GetClientReply struct {
	Val     string
	Err     rpc.Err
	Version rpc.Tversion
}

type PutClientArgs struct {
	Key         string
	Val         string
	Version     rpc.Tversion
	ClientId    int64
	RequestId   int64
	CommandType int //0 for get 1 for put
}

type PutClientReply struct {
	Err rpc.Err
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{Clnt: clnt,
		Servers:   servers,
		mu:        sync.Mutex{},
		RequestId: 0,
		ClientId:  rand.Int63()}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.Clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	ck.mu.Lock()
	//clientId := ck.ClientId
	//requestId := ck.RequestId
	//log.Printf("client id is %d", clientId)
	//log.Printf("client requestId is %d", requestId)
	ck.RequestId++
	lastLeader := ck.LastLeader
	ck.mu.Unlock()
	for {
		for serverIndex := 0; serverIndex < len(ck.Servers); serverIndex++ {
			request := &rpc.GetArgs{Key: key}
			reply := &rpc.GetReply{}
			toCall := (serverIndex + lastLeader) % len(ck.Servers) //first call the last leader then subsequent ones.
			ok := ck.Clnt.Call(ck.Servers[toCall], "KVServer.Get", request, reply)
			if !ok {
				time.Sleep(200 * time.Millisecond)
			} else {
				if reply.Err == rpc.ErrNoKey {
					// if err no key return, otherwise keep retrying.
					return "", reply.Version, reply.Err
				}
				if reply.Err == rpc.OK {
					ck.mu.Lock()
					ck.LastLeader = toCall
					ck.mu.Unlock()
					log.Printf("get returned ok")
					return reply.Value, reply.Version, reply.Err
				} else {
					continue
				}
			}
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.Clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	retriedPut := false
	ck.mu.Lock()
	//clientId := ck.ClientId
	//requestId := ck.RequestId
	//log.Printf("client id is %d", clientId)
	//log.Printf("client requestId is %d", requestId)
	ck.RequestId++
	lastLeader := ck.LastLeader
	ck.mu.Unlock()
	for {
		for serverIndex := 0; serverIndex < len(ck.Servers); serverIndex++ {
			request := &rpc.PutArgs{Key: key, Value: value}
			reply := &PutClientReply{}
			toCall := (serverIndex + lastLeader) % len(ck.Servers)
			ok := ck.Clnt.Call(ck.Servers[toCall], "KVServer.Put", request, reply)
			if !ok {
				retriedPut = true
				time.Sleep(200 * time.Millisecond)
			} else {
				//case based on err reply.
				switch reply.Err {
				case rpc.OK:
					ck.mu.Lock()
					ck.LastLeader = toCall
					ck.mu.Unlock()
					log.Printf("put returned ok")
					return rpc.OK
				case rpc.ErrNoKey:
					return rpc.ErrNoKey
				case rpc.ErrVersion:
					if retriedPut == false {
						return rpc.ErrVersion
					} else {
						//if this is on a retry, it may have been put into the kv store.
						return rpc.ErrMaybe
					}
				case rpc.ErrWrongLeader:
					retriedPut = true
					continue
				}
			}
		}
	}
}
