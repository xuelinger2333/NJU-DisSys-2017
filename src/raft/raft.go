package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// LogEntry is a single Raft log entry. log[0] is a sentinel with Term=0
// so that LastLogIndex/LastLogTerm in the up-to-date check are well-defined
// for an empty log.
type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	roleFollower  = 0
	roleCandidate = 1
	roleLeader    = 2

	heartbeatInterval = 120 * time.Millisecond
	tickerInterval    = 10 * time.Millisecond
	electionMinMs     = 300
	electionJitterMs  = 150
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Persistent state on all servers (Figure 2).
	currentTerm int
	votedFor    int        // -1 means none in this term
	log         []LogEntry // log[0] is a sentinel; real entries start at index 1

	// Volatile state on all servers.
	role        int
	commitIndex int // Part II
	lastApplied int // Part II

	// Volatile state on leaders (Part II uses these; declared now).
	nextIndex  []int
	matchIndex []int

	// Implementation aux.
	electionResetAt time.Time
	applyCh         chan ApplyMsg
	dead            int32 // set by Kill via atomic
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == roleLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

// RequestVote RPC arguments structure (Figure 2).
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure (Figure 2).
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs is the heartbeat / log-replication RPC arg (Figure 2).
// In Part I, Entries is always empty.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply is the heartbeat / log-replication RPC reply.
// ConflictTerm/ConflictIndex are a hint for the leader to skip a whole term
// of mismatched entries in one round-trip (canonical 6.824 fast backoff).
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int // -1 means PrevLogIndex was past follower's log end
	ConflictIndex int // first index with ConflictTerm in follower's log, or len(log) for the -1 case
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = roleFollower
	}

	last := len(rf.log) - 1
	lastTerm := rf.log[last].Term
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= last)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.electionResetAt = time.Now()
	}
	reply.Term = rf.currentTerm
}

// AppendEntries RPC handler implementing Figure 2 receiver logic with
// the ConflictTerm/ConflictIndex fast-backoff hint.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	reply.ConflictTerm = 0
	reply.ConflictIndex = 0

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.role = roleFollower
	rf.electionResetAt = time.Now()
	reply.Term = rf.currentTerm

	// Consistency check with conflict hint.
	if len(rf.log) <= args.PrevLogIndex {
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 0 && rf.log[i-1].Term == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// Merge entries: keep matching prefix, truncate-and-append only on actual
	// term conflict. Stale duplicate AEs must not wipe an already-matched suffix.
	for k := 0; k < len(args.Entries); k++ {
		idx := args.PrevLogIndex + 1 + k
		if idx >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[k:]...)
			break
		}
		if rf.log[idx].Term != args.Entries[k].Term {
			rf.log = rf.log[:idx]
			rf.log = append(rf.log, args.Entries[k:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNew {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNew
		}
	}

	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != roleLeader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.persist()
	go rf.kickReplicate(rf.currentTerm)
	return index, rf.currentTerm, true
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func randomElectionTimeout() time.Duration {
	return time.Duration(electionMinMs+rand.Intn(electionJitterMs)) * time.Millisecond
}

// ticker checks each tickerInterval whether the election timeout has elapsed
// and starts a new election if so. Runs as a single long-lived goroutine.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(tickerInterval)
		rf.mu.Lock()
		if rf.role != roleLeader && time.Since(rf.electionResetAt) >= randomElectionTimeout() {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// startElection is invoked with rf.mu held and returns with it still held.
// It transitions to Candidate, persists, then fans out RequestVote RPCs
// without holding the lock.
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.role = roleCandidate
	rf.votedFor = rf.me
	rf.electionResetAt = time.Now()
	rf.persist()

	termSnap := rf.currentTerm
	last := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:         termSnap,
		CandidateId:  rf.me,
		LastLogIndex: last,
		LastLogTerm:  rf.log[last].Term,
	}
	votes := 1

	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		go func(peer int) {
			var reply RequestVoteReply
			if !rf.sendRequestVote(peer, args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.role = roleFollower
				rf.persist()
				return
			}
			if rf.currentTerm != termSnap || rf.role != roleCandidate {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.becomeLeader(termSnap)
				}
			}
		}(p)
	}
}

// becomeLeader transitions to Leader and spawns the heartbeat goroutine.
// Caller must hold rf.mu.
func (rf *Raft) becomeLeader(termSnap int) {
	rf.role = roleLeader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	go rf.heartbeat(termSnap)
}

// buildAEArgs builds the AppendEntries args targeted at peer i, based on the
// current nextIndex[i]. Caller must hold rf.mu. The Entries slice is freshly
// allocated so subsequent log mutations cannot alias the in-flight payload.
func (rf *Raft) buildAEArgs(i, leaderTerm int) AppendEntriesArgs {
	prevIdx := rf.nextIndex[i] - 1
	prevTerm := rf.log[prevIdx].Term
	src := rf.log[rf.nextIndex[i]:]
	entries := make([]LogEntry, len(src))
	copy(entries, src)
	return AppendEntriesArgs{
		Term:         leaderTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

// heartbeat is the leader's replication driver: every heartbeatInterval it
// builds per-peer AppendEntries args from nextIndex[i] (which may carry real
// entries or be empty), fans them out, and exits when no longer leader of
// leaderTerm.
func (rf *Raft) heartbeat(leaderTerm int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != roleLeader || rf.currentTerm != leaderTerm {
			rf.mu.Unlock()
			return
		}
		argsByPeer := make([]AppendEntriesArgs, len(rf.peers))
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			argsByPeer[i] = rf.buildAEArgs(i, leaderTerm)
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.replicateOnce(i, leaderTerm, argsByPeer[i])
		}

		time.Sleep(heartbeatInterval)
	}
}

// kickReplicate triggers an immediate replication round to all peers. Called
// from Start so a new entry doesn't have to wait up to heartbeatInterval.
func (rf *Raft) kickReplicate(leaderTerm int) {
	rf.mu.Lock()
	if rf.role != roleLeader || rf.currentTerm != leaderTerm {
		rf.mu.Unlock()
		return
	}
	argsByPeer := make([]AppendEntriesArgs, len(rf.peers))
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		argsByPeer[i] = rf.buildAEArgs(i, leaderTerm)
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateOnce(i, leaderTerm, argsByPeer[i])
	}
}

// replicateOnce sends one AppendEntries to peer i and processes the reply:
// step down on higher term, advance matchIndex/nextIndex on success and try to
// commit, apply ConflictTerm/ConflictIndex backoff on failure.
func (rf *Raft) replicateOnce(i, leaderTerm int, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	if !rf.sendAppendEntries(i, args, &reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != leaderTerm || rf.role != roleLeader {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = roleFollower
		rf.persist()
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[i] {
			rf.matchIndex[i] = newMatch
		}
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		rf.advanceCommit()
		return
	}

	// Failure: apply conflict-hint backoff.
	if reply.ConflictTerm == -1 {
		rf.nextIndex[i] = reply.ConflictIndex
	} else {
		j := len(rf.log) - 1
		for j >= 0 && rf.log[j].Term != reply.ConflictTerm {
			j--
		}
		if j >= 0 {
			rf.nextIndex[i] = j + 1
		} else {
			rf.nextIndex[i] = reply.ConflictIndex
		}
	}
	if rf.nextIndex[i] < 1 {
		rf.nextIndex[i] = 1
	}
}

// advanceCommit (called under mu) walks log indices from the tail looking for
// the highest N > commitIndex where a majority has matchIndex[i] >= N AND the
// entry at N is from currentTerm (Figure 8 safety: a leader may only directly
// commit entries from its own term).
func (rf *Raft) advanceCommit() {
	if rf.role != roleLeader {
		return
	}
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		if rf.log[N].Term != rf.currentTerm {
			continue
		}
		cnt := 0
		for _, m := range rf.matchIndex {
			if m >= N {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitIndex = N
			return
		}
	}
}

// applier delivers committed entries to applyCh in strictly monotonic order.
// Single-writer of lastApplied; lock is released across the channel send to
// avoid deadlock if the tester's reader is slow.
func (rf *Raft) applier() {
	for !rf.killed() {
		time.Sleep(tickerInterval)
		rf.mu.Lock()
		var msgs []ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msgs = append(msgs, ApplyMsg{
				Index:   rf.lastApplied,
				Command: rf.log[rf.lastApplied].Command,
			})
		}
		rf.mu.Unlock()
		for _, m := range msgs {
			rf.applyCh <- m
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}}
	rf.role = roleFollower
	rf.applyCh = applyCh
	rf.electionResetAt = time.Now()

	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().UnixNano() ^ int64(me))

	go rf.ticker()
	go rf.applier()

	return rf
}
