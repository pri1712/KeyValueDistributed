package rsm

import (
	"kvraft/src/kvsrv1/rpc"
	"kvraft/src/labrpc"
	raft "kvraft/src/raft1"
	"kvraft/src/raftapi"
	tester "kvraft/src/tester1"
	"log"
	"sync"
	"time"
)

var useRaftStateMachine bool // to plug in another raft besides raft1

type Op struct {
	EventId     int
	Me          int
	Request     any
	RequestType int
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
	EventId int
	Ch      chan pendingResult
}

type pendingResult struct {
	Err rpc.Err
	Val any
}

type RSM struct {
	mu                sync.Mutex
	me                int
	rf                raftapi.Raft
	applyCh           chan raftapi.ApplyMsg
	maxraftstate      int // snapshot if log grows this big
	sm                StateMachine
	currenteventId    int
	pendingMap        map[int]*pendingEntry
	lastSnapshotIndex int
	lastSnapshotTerm  int
	lastSnapshot      []byte
	persister         *tester.Persister
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
		me:                me,
		maxraftstate:      maxraftstate,
		applyCh:           make(chan raftapi.ApplyMsg),
		sm:                sm,
		pendingMap:        make(map[int]*pendingEntry),
		lastSnapshotIndex: 0,
		lastSnapshotTerm:  0,
		lastSnapshot:      nil,
		persister:         persister,
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
			//log.Printf("channel reader got msg details as follows;"+
			//	" msg command index : %v"+
			//	" msg command valid : %v"+
			//	" msg command details : %v", msg.CommandIndex, msg.CommandValid, msg.Command.(Op),
			//)
			if msg.CommandValid {
				appliedOperation, ok := msg.Command.(Op)
				if !ok {
					continue
				}
				finalResult := rsm.sm.DoOp(appliedOperation.Request)
				//log.Printf("command is %v", msg.Command)
				//snapshot details capture.
				log.Printf("final result: %v", finalResult)
				if finalResult == nil {
					//log.Printf("there was an incorrect command in doOp, please check.")
				}
				rsm.mu.Lock()
				rsm.lastSnapshotIndex = msg.CommandIndex
				rsm.lastSnapshotTerm = msg.SnapshotTerm
				rsm.mu.Unlock()
				if rsm.maxraftstate >= 0 && rsm.maxraftstate <= rsm.rf.PersistBytes() {
					snapshot := rsm.sm.Snapshot()
					rsm.rf.Snapshot(msg.CommandIndex, snapshot)
				}
				//log.Printf("applied op eventid: %v", appliedOperation.EventId)
				//log.Printf("msg command index: %v", msg.CommandIndex)
				rsm.mu.Lock()
				entry, exists := rsm.pendingMap[msg.CommandIndex] //checking if it exists in the pending map.
				rsm.mu.Unlock()
				if !exists {
					//log.Printf("msg.commandIndex: %v", msg.CommandIndex)
					//log.Printf("cannot find the same eventId in map")
					continue
				} else {
					ch := entry.Ch
					var out pendingResult
					if entry.EventId != appliedOperation.EventId {
						//different command has appeared at the index replied by start or the term has changed.
						//log.Printf("not the leader")
						out = pendingResult{Err: rpc.ErrWrongLeader, Val: nil}
					} else {
						//log.Printf("final result %v", finalResult)
						out = pendingResult{Err: rpc.OK, Val: finalResult}
					}
					ch <- out
					rsm.mu.Lock()
					if cur, ok := rsm.pendingMap[msg.CommandIndex]; ok && cur == entry {
						//log.Printf("deleting %v", msg.CommandIndex)
						delete(rsm.pendingMap, msg.CommandIndex)
					}
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
	restoredSnapshot := rsm.persister.ReadSnapshot()
	if restoredSnapshot != nil && len(restoredSnapshot) > 0 {
		log.Printf("restored snapshot: %v", restoredSnapshot)
		rsm.sm.Restore(restoredSnapshot)
		rsm.lastSnapshot = restoredSnapshot
	}
	log.Printf("request is %v", req)
	currentOp := Op{EventId: eventId, Me: rsm.me, Request: req}
	pending := pendingEntry{EventId: eventId, Ch: make(chan pendingResult, 1)}
	//log.Printf("operation %v submitted", currentOp)
	index, _, isLeader := rsm.rf.Start(currentOp)
	//log.Printf("index where it will get inserted is: %v", index)
	if !isLeader {
		//only works if the leader is not alone in a partition, if it is it wont know about new terms.
		return rpc.ErrWrongLeader, nil
	}
	rsm.mu.Lock()
	rsm.pendingMap[index] = &pending //mapping index to eventID. so we can check if the same eventId got inserted at
	//that index.
	rsm.mu.Unlock()

	select {
	case res := <-pending.Ch:
		log.Printf("sending back res : %v", res)
		return res.Err, res.Val
	case <-time.After(5 * time.Second):
		rsm.mu.Lock()
		delete(rsm.pendingMap, index)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

}
