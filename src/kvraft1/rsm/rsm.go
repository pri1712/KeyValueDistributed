package rsm

import (
	"kvraft/src/kvsrv1/rpc"
	"kvraft/src/labrpc"
	raft "kvraft/src/raft1"
	"kvraft/src/raftapi"
	tester "kvraft/src/tester1"
	"log"
	"sync"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	EventId int
	Me      int
	Request any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type pendingEntry struct {
	EventId    int
	LeaderTerm int
	ch         chan pendingResult
}

type pendingResult struct {
	Err rpc.Err
	Val any
}

type RSM struct {
	mu             sync.Mutex
	me             int
	rf             raftapi.Raft
	applyCh        chan raftapi.ApplyMsg
	maxraftstate   int // snapshot if log grows this big
	sm             StateMachine
	currenteventId int
	pendingMap     map[int]*pendingEntry
	// Your definitions here.
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pendingMap:   make(map[int]*pendingEntry),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.channelReader() //reads the applych to check for committed ops.
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) channelReader() {
	//read the applyCh channel here and send outputs if any to DoOp
	for {
		select {
		case msg, openCh := <-rsm.applyCh:
			//if we get something on the apply channel from raft.
			if !openCh {
				log.Println("channel reader closed")
				return
			}
			if msg.CommandValid {
				appliedOperation, ok := msg.Command.(Op)
				if !ok {
					continue
				}
				finalResult := rsm.sm.DoOp(appliedOperation.Request)
				log.Printf("applied op eventid: %v", appliedOperation.EventId)
				log.Printf("msg command index: %v", msg.CommandIndex)
				rsm.mu.Lock()
				entry, exists := rsm.pendingMap[msg.CommandIndex]
				rsm.mu.Unlock()
				if !exists {
					log.Printf("cannot find the same eventId in map")
					continue
				} else {
					ch := entry.ch
					term := entry.LeaderTerm
					currentTerm, isLeader := rsm.rf.GetState()
					log.Printf("current term is: %v and we are comp with : %v", currentTerm, term)
					if entry.EventId != appliedOperation.EventId || currentTerm != term || !isLeader {
						//different command has appeared at the index replied by start or the term has changed.
						ch <- pendingResult{Err: rpc.ErrWrongLeader, Val: nil}
					} else {
						ch <- pendingResult{Err: rpc.OK, Val: finalResult}
					}
					rsm.mu.Lock()
					delete(rsm.pendingMap, msg.CommandIndex)
					rsm.mu.Unlock()
				}
			} else if msg.SnapshotValid {
				log.Printf("Snapshot is valid")
				continue
			}
		}
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	//need to call raft.start here
	rsm.mu.Lock()
	eventId := rsm.currenteventId
	rsm.currenteventId++
	rsm.mu.Unlock()
	currentOp := Op{EventId: eventId, Me: rsm.me, Request: req}
	index, term, isLeader := rsm.rf.Start(currentOp)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}
	pending := pendingEntry{EventId: eventId, LeaderTerm: term, ch: make(chan pendingResult, 1)}
	rsm.mu.Lock()
	rsm.pendingMap[index] = &pending //mapping index to eventID.
	rsm.mu.Unlock()
	select {
	case res := <-pending.ch:
		//if the reader sent errwrong leader return that, else return the result from DoOp.
		//log.Printf("in case 1")
		log.Printf("res: %v,%v", res.Err, res.Val)
		return res.Err, res.Val
		//case <-time.After(time.Second):
		//	rsm.mu.Lock()
		//	delete(rsm.pendingMap, index)
		//	rsm.mu.Unlock()
		//	//log.Printf("in case 2")

	}
}
