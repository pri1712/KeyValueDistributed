package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"kvraft/src/labgob"
	"log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	_ "kvraft/src/labgob"
	"kvraft/src/labrpc"
	"kvraft/src/raftapi"
	"kvraft/src/tester1"
	"kvraft/src/utils"
)

const (
	Follower State = iota
	Candidate
	Leader
)

const MinTime int = 600
const MaxTime int = 900

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's ServerState
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted ServerState
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	//persist to disk
	EventLogs   []LogEntry
	VotedFor    int
	CurrentTerm int
	//volatile information
	CommitIndex int
	LastApplied int
	//for leader only.
	NextIndex  []int
	MatchIndex []int
	//ServerState information
	ServerState State
	//election information
	ElectionTimeout time.Duration
	LastHeartBeat   time.Time
	VoteCount       int
	StopHeartBeat   chan struct{}
	// Look at the paper's Figure 2 for a description of what
	// ServerState a Raft server must maintain.
	//to talk to the application
	ApplicationChanel chan raftapi.ApplyMsg
	//to have conditional run of the applier whenever there is a change in the commit index.
	Cond *sync.Cond
	//to store the latest snapshot received from the application.
	LastIncludedTerm  int
	LastIncludedIndex int
	LastSnapshot      []byte
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	SnapshotData      []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

type InstallSnapshotReply struct {
	Term int
}

type State int

type LogEntry struct {
	Term    int
	Command interface{}
}

// RequestVoteArgs structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int //current Term on candidate server.
	CandidateId  int //id of the candidate server.
	LastLogIndex int //last index the server has filled up in its log.
	LastLogTerm  int //Term of the item at the last log index.
}

// RequestVoteReply  structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //CurrentTerm, for candidate to update itself
	VoteGranted bool //if vote was given or not to the current candidate.
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.CurrentTerm
	if rf.ServerState == Leader {
		isleader = true
	}
	return term, isleader
}

//func dumpAndPanic(format string, args ...interface{}) {
//	log.Printf("[INDEX ERROR] "+format, args...)
//	log.Printf("[STACK TRACE]\n%s", debug.Stack())
//	panic("invalid index detected in Raft")
//}

// GetSnapshotLogIndex must be called from within a lock.
func (rf *Raft) GetSnapshotLogIndex(abs int) int {
	rel := abs - rf.LastIncludedIndex
	if rel < 0 || rel >= len(rf.EventLogs) {
		//pc, file, line, ok := runtime.Caller(1)
		//if !ok {
		//	log.Printf("Error while getting function callerr")
		//	return -1
		//}
		//f := runtime.FuncForPC(pc)
		//if f == nil {
		//	fmt.Println("Could not get function details")
		//	return -1
		//}
		//log.Printf("Called by: %s (File: %s, Line: %d)\n", f.Name(), file, line)
		log.Printf("[INDEX ERROR] abs=%d lastInc=%d rel=%d len=%d", abs, rf.LastIncludedIndex, rel, len(rf.EventLogs))
	}
	return rel
}

// GetSnapshotLogTerm must be calle from within a lock
func (rf *Raft) GetSnapshotLogTerm(index int) int {
	if index == rf.LastIncludedIndex {
		return rf.LastIncludedTerm
	}
	rel := index - rf.LastIncludedIndex
	if rel < 0 || rel >= len(rf.EventLogs) {
		log.Println("GetSnapshotLogTerm out of boundary")
	}
	return rf.EventLogs[rel].Term
}

// save Raft's persistent ServerState to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	//to save to the disk.
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	writeBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writeBuffer)
	err := encoder.Encode(rf.CurrentTerm)
	if err != nil {
		log.Printf("error encoding current term: %v", err)
		return
	}
	err = encoder.Encode(rf.VotedFor)
	if err != nil {
		log.Printf("error encoding voted for: %v", err)
		return
	}
	err = encoder.Encode(rf.EventLogs)
	if err != nil {
		log.Printf("error encoding event logs: %v", err)
		return
	}
	err = encoder.Encode(rf.LastIncludedTerm)
	if err != nil {
		log.Printf("error encoding last log term: %v", err)
		return
	}
	err = encoder.Encode(rf.LastIncludedIndex)
	if err != nil {
		log.Printf("error encoding last log index: %v", err)
		return
	}
	raftState := writeBuffer.Bytes()
	rf.persister.Save(raftState, rf.LastSnapshot)
}

// restore previously persisted ServerState.
func (rf *Raft) readPersist(data []byte) {
	//to read from the disk.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Println("readPersist")
	if data == nil || len(data) < 1 { // bootstrap without any ServerState?
		log.Println("readPersist empty data")
		return
	}
	readBuffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(readBuffer)
	var CurrentTerm int
	var VotedFor int
	var EventLogs []LogEntry
	var LastIncludedIndex int
	var LastIncludedTerm int
	if decoder.Decode(&CurrentTerm) != nil || decoder.Decode(&VotedFor) != nil || decoder.Decode(&EventLogs) != nil || decoder.Decode(&LastIncludedTerm) != nil || decoder.Decode(&LastIncludedIndex) != nil {
		//log.Printf("readPersist failed while decoding")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.EventLogs = EventLogs
		rf.LastIncludedTerm = LastIncludedTerm
		rf.LastIncludedIndex = LastIncludedIndex
		if rf.persister.SnapshotSize() != 0 {
			rf.LastSnapshot = rf.persister.ReadSnapshot()
		}
		if len(rf.LastSnapshot) > 0 {
			//log.Printf("last included index and term %d,%d", rf.LastIncludedIndex, rf.LastIncludedTerm)
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.LastSnapshot,
				SnapshotTerm:  rf.LastIncludedTerm,
				SnapshotIndex: rf.LastIncludedIndex,
			}
			go func() { rf.ApplicationChanel <- msg }()
		}
		if rf.LastIncludedIndex > 0 {
			rf.CommitIndex = rf.LastIncludedIndex
			rf.LastApplied = rf.LastIncludedIndex
		}
		//log.Printf("persisted logs for server %d are %v", rf.me, rf.EventLogs)
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible
// so basically trim it upto the last included index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.LastIncludedIndex {
		//we have already created a snapshot of this. reject.
		return
	}
	startIndex := rf.GetSnapshotLogIndex(index) //normalizing the index access.
	if startIndex < 0 || startIndex >= len(rf.EventLogs) || index > rf.LastApplied {
		//cant be more than size of our event logs and also cant be after the last applied index, can only snapshot
		//applied entries.
		return
	}
	// lastincludedindex index update.
	rf.LastIncludedIndex = index
	//lastincluded term update
	//log.Printf("here in Snapshot")
	rf.LastIncludedTerm = rf.EventLogs[startIndex].Term //position is relative to the prior last included index.
	newEventLogs := []LogEntry{{rf.LastIncludedTerm, nil}}
	//log.Printf("here in Snapshot2")
	if startIndex+1 > len(rf.EventLogs) {
		log.Printf("[OUT OF BOUNDS ACCESS] startIndex is higher than event logs length.")
	}
	newEventLogs = append(newEventLogs, rf.EventLogs[startIndex+1:]...)
	rf.EventLogs = newEventLogs
	rf.LastSnapshot = snapshot
	rf.persist()
	//persisted to disk
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// InstallSnapshot this is on the receiving end of the installsnapshot RPC, have to handle edge cases and trim logs.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//log.Println("InstallSnapshot called")

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}
	reply.Term = rf.CurrentTerm

	//reject if older term.
	if args.Term < rf.CurrentTerm {
		return
	}

	//update the current term if newer term found
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.ServerState = Follower
		rf.VoteCount = 0
	}

	//if already snapshotted, reject.
	if args.LastIncludedIndex <= rf.LastIncludedIndex {
		return
	}

	oldEventLogs := rf.EventLogs
	oldLastIncludedIndex := rf.LastIncludedIndex
	// relative index of snapshot boundary in old logs
	relIndex := args.LastIncludedIndex - oldLastIncludedIndex
	//dummy entry + entries after the snapshot point.
	newEventLogs := []LogEntry{{Term: args.LastIncludedTerm, Command: nil}}
	if relIndex >= 0 && relIndex+1 < len(oldEventLogs) {
		newEventLogs = append(newEventLogs, oldEventLogs[relIndex+1:]...)
	}
	rf.EventLogs = newEventLogs

	// Update snapshot metadata
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.LastSnapshot = args.SnapshotData

	// Advance commit/applied
	rf.CommitIndex = args.LastIncludedIndex
	rf.LastApplied = args.LastIncludedIndex

	// Persist
	rf.persist()

	// Release lock before notifying application
	rf.mu.Unlock()

	// Send one snapshot ApplyMsg
	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.SnapshotData,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.ApplicationChanel <- msg

	rf.mu.Lock()
}

func (rf *Raft) ReplicateLogsToFollower(server int, term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.CurrentTerm != term || rf.ServerState != Leader {
			//log.Printf("SendEventLogs failed: term %v, command %v", rf.CurrentTerm, eventCommand)
			rf.mu.Unlock()
			return
		}
		isLeader := rf.ServerState == Leader
		me := rf.me
		commitIndex := rf.CommitIndex
		currentSnapshot := rf.LastSnapshot
		lastIncludedTerm := rf.LastIncludedTerm
		lastIncludedIndex := rf.LastIncludedIndex
		if isLeader {
			nextindex := rf.NextIndex[server] //from where we have to send the logs to this server
			if nextindex < 1 {
				nextindex = 1
			}
			//log.Printf("SendEventLogs peers[%v]: nextindex: %v", server, nextindex)
			prevlogindex := nextindex - 1
			//log.Printf("previous log index: %v", prevlogindex)
			prevlogterm := 0
			if prevlogindex >= lastIncludedIndex {
				prevlogterm = rf.GetSnapshotLogTerm(prevlogindex)
			} else {
				prevlogterm = lastIncludedTerm
			}
			//log.Printf("here")
			//log.Printf("nextindex : %v", nextindex)

			//send an installsnapshot rpc.
			if nextindex <= rf.LastIncludedIndex {
				//log.Printf("called install snapshot for server %d from server %d", server, rf.me)
				request := InstallSnapshotArgs{
					Term:              term,
					LeaderId:          me,
					SnapshotData:      currentSnapshot,
					LastIncludedIndex: lastIncludedIndex,
					LastIncludedTerm:  lastIncludedTerm,
				}
				rf.mu.Unlock()
				reply := InstallSnapshotReply{}
				//log.Printf("sending install snapshot request: %v", request)
				ok := rf.peers[server].Call("Raft.InstallSnapshot", &request, &reply)
				if !ok {
					log.Printf("InstallSnapshot failed for server: %v", server)
					time.Sleep(50 * time.Millisecond)
				} else {
					rf.mu.Lock()
					//log.Printf("InstallSnapshot succeeded for server: %v", server)
					rf.NextIndex[server] = rf.LastIncludedIndex + 1
					rf.MatchIndex[server] = rf.LastIncludedIndex
					rf.mu.Unlock()
				}
				continue
			}
			if nextindex-rf.LastIncludedIndex < 0 || nextindex-rf.LastIncludedIndex > len(rf.EventLogs) {
				log.Fatalf("[Leader %d] invalid slice: next=%d lastInc=%d len=%d",
					rf.me, nextindex, rf.LastIncludedIndex, len(rf.EventLogs))
			}
			//log.Printf("here in ReplicateLogsToFollower")
			sendEntries := append([]LogEntry(nil), rf.EventLogs[nextindex-rf.LastIncludedIndex:]...)
			//log.Printf("sending entries %v from server %v", sendEntries, rf.me)
			//log.Printf("event logs for server %v are %v", rf.me, rf.EventLogs)
			//log.Printf("here2")
			request := AppendEntriesArgs{
				Term:         term,
				LeaderId:     me,
				PrevLogIndex: prevlogindex,
				PrevLogTerm:  prevlogterm,
				Entries:      sendEntries,
				LeaderCommit: commitIndex,
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply
			ok := rf.peers[server].Call("Raft.AppendEntries", &request, &reply)
			if !ok {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			rf.mu.Lock()
			if reply.Term > term || rf.ServerState != Leader {
				//log.Printf("No longer in same term or no longer a leader.")
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.VoteCount = 0
				rf.ServerState = Follower
				if rf.StopHeartBeat != nil {
					close(rf.StopHeartBeat)
					rf.StopHeartBeat = nil
				}
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				//log.Printf("Successfully appended logs to server %v", server)
				//log.Printf("the logs are : %v", request.Entries)
				match := request.PrevLogIndex + len(request.Entries)
				if match > rf.MatchIndex[server] {
					rf.MatchIndex[server] = match
				}
				if rf.NextIndex[server] < match+1 {
					rf.NextIndex[server] = match + 1
				}

				//max possible real index if there was no normalization due to snapshotting.
				maxRealIndex := rf.LastIncludedIndex + len(rf.EventLogs) - 1
				for N := rf.CommitIndex + 1; N <= maxRealIndex; N++ {
					if rf.GetSnapshotLogTerm(N) != rf.CurrentTerm {
						continue
					}
					cnt := 1 // include leader
					for i := range rf.peers {
						if i != rf.me && rf.MatchIndex[i] >= N {
							cnt++
						}
					}
					if cnt > len(rf.peers)/2 {
						rf.CommitIndex = N
						rf.Cond.Signal()
					}
				}
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				continue
			} else {
				//backoff logic. skip over all the same terms, to reduce the number of RPC calls.
				newNextIndex := rf.NextIndex[server]
				conflictTerm := reply.ConflictTerm
				conflictIndex := reply.ConflictIndex
				if conflictTerm != -1 {
					//there is some term that is conflicting.
					if conflictIndex <= rf.LastIncludedIndex {
						//install snapshot rpc needs to be sent.
						request := InstallSnapshotArgs{
							Term:              term,
							LeaderId:          me,
							SnapshotData:      currentSnapshot,
							LastIncludedIndex: lastIncludedIndex,
							LastIncludedTerm:  lastIncludedTerm,
						}
						rf.mu.Unlock()
						reply := InstallSnapshotReply{}
						ok := rf.peers[server].Call("Raft.InstallSnapshot", &request, &reply)
						if !ok {
							//log.Printf("InstallSnapshot failed for server: %v", server)
							time.Sleep(50 * time.Millisecond)
						} else {
							rf.mu.Lock()
							//log.Printf("InstallSnapshot succeeded for server: %v", server)
							rf.NextIndex[server] = rf.LastIncludedIndex + 1
							rf.MatchIndex[server] = rf.LastIncludedIndex
						}
					}
					lastConflictIndex := -1
					maxRealIndex := rf.LastIncludedIndex + len(rf.EventLogs) - 1
					for i := maxRealIndex; i > lastIncludedIndex; i-- {
						if rf.EventLogs[rf.GetSnapshotLogIndex(i)].Term == conflictTerm {
							lastConflictIndex = i
							break
						}
					}
					if lastConflictIndex != -1 {
						newNextIndex = lastConflictIndex + 1
					} else if conflictIndex >= 1 {
						newNextIndex = conflictIndex
					}
				} else if conflictIndex != -1 {
					newNextIndex = conflictIndex
				}
				if newNextIndex < 1 {
					newNextIndex = 1
				}
				rf.NextIndex[server] = newNextIndex
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return -1, 0, false
	}
	index := -1 //where we plan to insert the command sent by the applicn.
	term := rf.CurrentTerm
	isLeader := rf.ServerState == Leader
	if isLeader {
		index = len(rf.EventLogs) + rf.LastIncludedIndex
		rf.EventLogs = append(rf.EventLogs, LogEntry{Term: term, Command: command})
		rf.persist()
		//go rf.SendEventLogs(term, command)
	} else {
		return -1, term, false
	}
	return index, term, isLeader

}

// AppendEntries , this is on the server that is on the receiving end of the RPC.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// default reply
	reply.Term = rf.CurrentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	if rf.killed() {
		return
	}
	// term checks
	if args.Term < rf.CurrentTerm {
		return
	}
	if args.Term > rf.CurrentTerm {
		//if candidate or leader and your term is lower then gotta go back to being a follower
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.ServerState = Follower
		rf.VoteCount = 0
		rf.persist()
	}

	reply.Term = rf.CurrentTerm

	if rf.ServerState != Follower {
		rf.ServerState = Follower
		rf.VoteCount = 0
	}

	// consistency check: PrevLogIndex must exist and match PrevLogTerm
	maxRealIndex := rf.LastIncludedIndex + (len(rf.EventLogs) - 1)
	//if the prevlogindex is itself greater than the last index possible of the current node due to missed entries.
	if args.PrevLogIndex > maxRealIndex {
		reply.ConflictIndex = maxRealIndex + 1
		return
	}

	if rf.LastIncludedIndex > args.PrevLogIndex {
		//Handle snapshot case - need InstallSnapshot RPC
		reply.ConflictIndex = rf.LastIncludedIndex
		reply.ConflictTerm = -1
		return
	}

	//log.Printf("here in AppendEntries")
	if args.PrevLogIndex >= 0 && rf.EventLogs[rf.GetSnapshotLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		//log.Printf("here in AppendEntries2")
		conflictTerm := rf.EventLogs[rf.GetSnapshotLogIndex(args.PrevLogIndex)].Term
		reply.ConflictTerm = conflictTerm
		i := args.PrevLogIndex - rf.LastIncludedIndex
		//log.Printf("here in AppendEntries3")
		for i >= 0 && rf.EventLogs[i].Term == conflictTerm {
			i--
		}
		reply.ConflictIndex = i + 1 + rf.LastIncludedIndex //mismatched index
		return
	}
	prevRelIdx := rf.GetSnapshotLogIndex(args.PrevLogIndex)
	prevRelTerm := rf.GetSnapshotLogTerm(args.PrevLogIndex)
	//compare prevrelterm and the prevterm sent by the leader to see if they match.
	if prevRelTerm != args.PrevLogTerm {
		reply.ConflictTerm = prevRelTerm
		conflictTerm := reply.ConflictTerm
		relI := prevRelIdx
		//log.Printf("here in AppendEntries4")
		for relI >= 0 && rf.EventLogs[relI].Term == conflictTerm {
			relI--
		}
		firstConflictAbs := relI + 1 + rf.LastIncludedIndex //converting to absolute indexing
		reply.ConflictIndex = firstConflictAbs
		return
	}

	// Append / overwrite entries starting at insert index
	insertAbsolute := args.PrevLogIndex + 1
	if len(args.Entries) > 0 {
		needsPersistence := false
		//basically is  a log replication RPC not a heartbeat one.
		// find first position where entries diverge
		firstDiff := -1
		for i := 0; i < len(args.Entries); i++ {
			fIdx := insertAbsolute + i
			if fIdx > maxRealIndex {
				firstDiff = i
				break
			}
			fTerm := rf.GetSnapshotLogTerm(fIdx)
			if fTerm != args.Entries[i].Term {
				firstDiff = i
				break
			}
		}
		if firstDiff != -1 {
			// truncate follower log at divergence point and append remaining leader entries
			truncAbsolute := firstDiff + insertAbsolute
			//log.Printf("firstd diff and insertabsolute %v,%v", firstDiff, insertAbsolute)
			//log.Printf("server calling is %v", rf.me)
			truncRel := rf.GetSnapshotLogIndex(truncAbsolute)
			if truncRel < 0 {
				truncRel = 0
			}
			//log.Printf("here in AppendEntries5")
			//log.Printf("event logs upto truncrel: %v", rf.EventLogs[:truncRel])
			rf.EventLogs = append(rf.EventLogs[:truncRel], args.Entries[firstDiff:]...)
			needsPersistence = true
			maxRealIndex = rf.LastIncludedIndex + (len(rf.EventLogs) - 1)
		}
		if needsPersistence {
			rf.persist()
		}
	}

	// update commit index (volatile only)
	lastNewIndex := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.CommitIndex {
		if lastNewIndex < args.LeaderCommit {
			rf.CommitIndex = lastNewIndex
		} else {
			rf.CommitIndex = args.LeaderCommit
		}
		rf.Cond.Signal()
	}
	//log.Printf("logs for node %v are : %v", rf.me, rf.EventLogs)
	// heartbeat / reset election timer
	rf.LastHeartBeat = time.Now()
	reply.Success = true
	reply.Term = rf.CurrentTerm
}

func (rf *Raft) SendHeartBeatToPeers(server int, term int, leaderId int) {
	rf.mu.Lock()
	if rf.ServerState != Leader || rf.CurrentTerm != term || rf.killed() { //in case it's been modified by some other node.
		rf.mu.Unlock()
		return
	}
	prevlogindex := rf.NextIndex[server] - 1
	prevlogterm := 0
	if prevlogindex >= rf.LastIncludedIndex {
		//to avoid negative slice access.
		prevlogterm = rf.GetSnapshotLogTerm(prevlogindex)
	}
	request := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevlogindex,
		PrevLogTerm:  prevlogterm,
		Entries:      nil,
		LeaderCommit: rf.CommitIndex,
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	//log.Printf("Sending AppendEntries to %v", server)
	//log.Printf("peers: %v", rf.peers[server])
	ok := rf.peers[server].Call("Raft.AppendEntries", request, reply)
	if !ok {
		//log.Printf("SendHeartBeatToPeers failed for server %v", server)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.ServerState != Leader || term != rf.CurrentTerm {
			return
		}
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.ServerState = Follower
			rf.VoteCount = 0
			rf.persist()
			return
		}
	}
}

func (rf *Raft) SendHeartbeatImmediate() {
	rf.mu.Lock()
	//log.Printf("SendHeartbeatImmediate")
	if rf.ServerState != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	term := rf.CurrentTerm
	leaderId := rf.me
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i == leaderId {
			//log.Printf("Leader inside sendheartbeatimmediate is: %v", leaderId)
			continue
		}
		//log.Printf("Sending heartbeat to %v", i)
		go rf.SendHeartBeatToPeers(i, term, leaderId) //send concrrently to increase speed.
	}

}

func (rf *Raft) PeriodicHeartbeats(stopCh chan struct{}) {
	heartbeatInterval := 100 * time.Millisecond //to try and reduce RPC count.
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	//log.Printf("Heartbeat periodic every %v", heartbeatInterval)
	//when just became a leader send it right away.
	for {
		select {
		// if not the leader. cant send out shit.
		case <-ticker.C:
			rf.mu.Lock()
			if rf.ServerState != Leader || rf.killed() {
				//log.Printf("%v is no longer a leader and cant send out heartbeats", rf.me)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			//log.Printf("Sending out heartbeats now")
			rf.SendHeartbeatImmediate()
		case <-stopCh:
			return
		}
	}
}

// HandleVoteReplies handles the votes that a server receives.
func (rf *Raft) HandleVoteReplies(reply *RequestVoteReply, originalTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.VoteCount = 0
		rf.ServerState = Follower
		rf.persist()
		if rf.StopHeartBeat != nil {
			close(rf.StopHeartBeat)
			rf.StopHeartBeat = nil
		}
		return
	}
	if rf.ServerState != Candidate || rf.CurrentTerm != originalTerm {
		return
	}

	if reply.VoteGranted && rf.ServerState == Candidate {
		//vote counting.
		rf.VoteCount++
		if rf.VoteCount > len(rf.peers)/2 && rf.ServerState == Candidate {
			//become leader and send out rpc to all the other peers.
			rf.ServerState = Leader
			for i, _ := range rf.peers {
				//log.Printf("len of eventLogs: %v", len(rf.EventLogs))
				rf.NextIndex[i] = len(rf.EventLogs) + rf.LastIncludedIndex - 1
				rf.MatchIndex[i] = 0
				if i == rf.me {
					rf.MatchIndex[i] = len(rf.EventLogs) + rf.LastIncludedIndex - 1
					continue
				}
				go rf.ReplicateLogsToFollower(i, rf.CurrentTerm)
			}
			if rf.StopHeartBeat != nil {
				close(rf.StopHeartBeat)
			}
			rf.StopHeartBeat = make(chan struct{})
			go rf.PeriodicHeartbeats(rf.StopHeartBeat)
			log.Printf("Server %d became leader for term %d", rf.me, rf.CurrentTerm)
		}
	}
}

// RequestVote example RequestVote RPC handler.This is implemented on the servers receiving the RPC
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//log.Printf("In requesting vote")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	if rf.killed() {
		return
	}
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		//log.Printf("Term of candidate %d cannot be less than current Term %d\n", args.Term, rf.CurrentTerm)
		return
	}
	if args.Term > rf.CurrentTerm {
		//log.Printf("Server %d updating term from %d to %d, becoming follower", rf.me, rf.CurrentTerm, args.Term)
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.ServerState = Follower
		rf.VoteCount = 0
	}
	reply.Term = rf.CurrentTerm
	//grant vote only if;
	//1. Candidate's Term >= CurrentTerm
	//2. didnt vote for anyone yet or voted for this candidate
	//3. candidates log entries are atleast as up to date as ours.
	//by that I mean, If the logs have last entries with different terms, then
	//the log with the later(greater term number) Term is more up-to-date. If the logs
	//end with the same Term, then whichever log is longer is
	//more up-to-date
	nodeLastLogIndex := rf.LastIncludedIndex + len(rf.EventLogs) - 1
	nodeLastLogTerm := 0
	if nodeLastLogIndex >= 0 {
		nodeLastLogTerm = rf.GetSnapshotLogTerm(nodeLastLogIndex)
	}

	upToDate := false
	if args.LastLogTerm > nodeLastLogTerm ||
		(args.LastLogTerm == nodeLastLogTerm && args.LastLogIndex >= nodeLastLogIndex) {
		upToDate = true
	}
	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && upToDate {
		rf.VotedFor = args.CandidateId
		reply.VoteGranted = true
		rf.LastHeartBeat = time.Now()
	}
	rf.persist()
}

func (rf *Raft) StartElection() {
	//vote for self, increment the Term and send requestvote rpc
	//log.Printf("Start Election")
	rf.mu.Lock()
	defer utils.RecoverWithStackTrace("StartElection", rf.me)
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	rf.CurrentTerm++
	term := rf.CurrentTerm
	rf.ServerState = Candidate
	rf.VotedFor = rf.me
	rf.VoteCount = 1
	rf.LastHeartBeat = time.Now()
	lastLogIndex := rf.LastIncludedIndex + len(rf.EventLogs) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.GetSnapshotLogTerm(lastLogIndex)
	}
	rf.persist()
	rf.ElectionTimeout = time.Duration(rand.Intn(MaxTime-MinTime+1)+MinTime) * time.Millisecond
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			request := &RequestVoteArgs{term, rf.me, lastLogIndex, lastLogTerm}
			reply := &RequestVoteReply{}
			ok := rf.peers[server].Call("Raft.RequestVote", request, reply)
			if !ok {
				//log.Printf("RequestVote Failed")
			} else {
				rf.HandleVoteReplies(reply, term)
			}
		}(i)
	}
}

func (rf *Raft) applier() {
	if rf.killed() {
		rf.mu.Lock()
		close(rf.ApplicationChanel)
		rf.mu.Unlock()
	}
	for !rf.killed() {
		rf.mu.Lock()
		for rf.LastApplied >= rf.CommitIndex && !rf.killed() {
			rf.Cond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		for rf.LastApplied < rf.CommitIndex {
			rf.LastApplied++
			//log.Printf("Last applied index in the eventlog is %d", rf.LastApplied)
			//log.Printf("Last included index is %d", rf.LastIncludedIndex)
			//log.Printf("event logs for server %v : %v", rf.me, rf.EventLogs)
			idx := rf.LastApplied
			//log.Printf("last applied : %v", rf.LastApplied)
			//log.Printf("last included : %v", rf.LastIncludedIndex)
			newidx := rf.GetSnapshotLogIndex(idx)
			//log.Printf("New index after normalization: %d", newidx)
			//log.Printf("idx is %v", idx)
			//log.Printf("here in applier")
			cmd := rf.EventLogs[newidx].Command
			//log.Printf("logs for server %d are %v", rf.me, rf.EventLogs)
			msg := raftapi.ApplyMsg{CommandValid: true, Command: cmd, CommandIndex: idx}
			log.Printf("apply command on server %v : %v", rf.me, cmd)
			rf.mu.Unlock()
			rf.ApplicationChanel <- msg
			//log.Printf("Applied command: %v", cmd)
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) ticker() {
	//code necessary for 3A
	for rf.killed() == false {
		rf.mu.Lock()
		timeElapsed := time.Since(rf.LastHeartBeat)
		isLeader := rf.ServerState == Leader
		electionTimeout := rf.ElectionTimeout
		//log.Printf("event logs for server %v are %v ", rf.me, rf.EventLogs)
		rf.mu.Unlock()
		if !isLeader && timeElapsed > electionTimeout {
			rf.StartElection()
			//started a leader election process.
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent ServerState, and also initially holds the most
// recent saved ServerState, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) *Raft {
	// Your initialization code here (3A, 3B, 3C).
	rf := &Raft{}
	//log.Printf("Starting a server")
	rf.peers = peers
	rf.ApplicationChanel = applyCh
	rf.persister = persister
	rf.me = me
	rf.EventLogs = make([]LogEntry, 1)
	rf.EventLogs[0] = LogEntry{Term: 0, Command: nil}
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.ServerState = Follower
	rf.ElectionTimeout = time.Duration(rand.Intn(MaxTime-MinTime+1)+MinTime) * time.Millisecond
	//log.Printf("Timeout is: %v", rf.ElectionTimeout)
	rf.CommitIndex = 0
	rf.LastApplied = 0
	//log.Printf("length of peers: %v", len(rf.peers))
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	//log.Printf("len of nextIndex: %v", len(rf.NextIndex))
	rf.LastHeartBeat = time.Now()
	rf.StopHeartBeat = make(chan struct{})
	// initialize from ServerState persisted before a crash
	rf.LastIncludedTerm = 0
	rf.LastIncludedIndex = 0
	rf.Cond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	//go rf.SendHeartbeats()
	//log.Printf("Methods on *Raft: %+v", reflect.TypeOf(rf))
	//for i := 0; i < reflect.TypeOf(rf).NumMethod(); i++ {
	//	log.Printf("Method: %s", reflect.TypeOf(rf).Method(i).Name)
	//}
	return rf
}
