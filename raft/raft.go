// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"golang.org/x/exp/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// 用于随机数
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(uint64(time.Now().UnixNano()))),
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomizedElectionTimeout is a random number between
	// [electiontimeout, 2 * electiontimeout - 1]. It gets reset
	// when raft changes its state to follower or candidate.
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	totalTickCount  int	// 全局时钟	
	leaseStart			int // 租约开始时间
	leaseDeadline		int // 租约结束时间
	acks 						map[uint64]bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftlog := newLog(c.Storage)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	r := &Raft{
		id:               c.ID,
		Term:							hs.Term,
		Vote:							hs.Vote,
		Lead:             None,
		RaftLog:          raftlog,
		Prs:              make(map[uint64]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		acks:             make(map[uint64]bool),
	}

	peers := c.peers
	if len(peers) == 0 {
		peers = cs.Nodes
	}
	for _, p := range peers {
		r.Prs[p] = &Progress{Next: 1}
	}
		// 如果不是第一次启动而是从之前的数据进行恢复
	if !IsEmptyHardState(hs) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	// 启动都是follower状态
	r.becomeFollower(r.Term, None)

	r.DPrintf(log.DInfo, "%d start up", r.id)
	return r
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.totalTickCount++
  r.electionElapsed++
	switch r.State {
	case StateFollower, StateCandidate:
		if r.electionElapsed >= r.randomizedElectionTimeout {
			// 如果可以被提升为leader，同时选举时间也到了
			r.electionElapsed = 0
			// 开始选举
			r.DPrintf(log.DVote, "%d election timeout on term %d", r.id, r.Term)
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}

	case StateLeader:
		r.heartbeatElapsed++

		if r.electionElapsed > r.randomizedElectionTimeout {
			r.electionElapsed = 0
			// 超时，放弃 leadership transfer
			r.leadTransferee = None
		}

		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// 向集群中其他节点发送广播消息
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// 重置raft的一些状态
func (r *Raft) reset(term uint64) {
	if r.Term != term {
		// 如果是新的任期，那么保存任期号，同时将投票节点置空
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.leadTransferee = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
	r.votes = make(map[uint64]bool)
	for id := range r.Prs {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
	r.leaseStart = 0
	r.leaseDeadline = 0
	r.acks = make(map[uint64]bool)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	r.DPrintf(log.DState, "%d become follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// 因为进入candidate状态，意味着需要重新进行选举了，所以reset的时候传入的是Term+1
	r.reset(r.Term + 1)
	// 给自己投票
	r.Vote = r.id
	r.votes[r.id] = true
	r.State = StateCandidate
	r.DPrintf(log.DState, "%d become candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader

  // 附加一条空的日志
	r.appendEntry(&pb.Entry{Data: nil})
	// 复制空日志给其他节点
	r.bcastLogReplication()
	r.DPrintf(log.DState, "%d become leader at term %d", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
  // 处理消息的任期
	switch {
	case m.Term == 0:
		// 来自本地的消息
	case m.Term > r.Term:
		// 消息的Term大于节点当前的Term
		switch m.MsgType {
		// 如果是 Leader 发来的消息，则更新 r.Lead
		case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat, pb.MessageType_MsgSnapshot:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
    // 忽略任何term小于当前节点所在任期号的消息
    // 对于rpc类型的消息，还要发送拒绝消息
    switch m.MsgType {
    case pb.MessageType_MsgHeartbeat:
      r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeatResponse, Reject: true})
    case pb.MessageType_MsgRequestVote:
      r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
    case pb.MessageType_MsgAppend, pb.MessageType_MsgSnapshot:
      msg := pb.Message{
        MsgType: pb.MessageType_MsgAppendResponse,
        Term: r.Term,
        To: m.From,
        Index: r.RaftLog.LastIndex(),
        Reject: true,
      }
      r.send(msg)
    }
    return nil
	}

  // 进入各种状态下自己定制的状态机函数
  switch r.State {
  case StateFollower:
    return r.stepFollower(m)
  case StateCandidate:
    return r.stepCandidate(m)
  case StateLeader:
    return r.stepLeader(m)
  }
	return nil
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{
			Match: 0,
			Next: 1,
		}
		r.DPrintf(log.DConf, "%d add node %d, now Prs=%v", r.id, id, r.Prs)
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		r.DPrintf(log.DConf, "%d remove node %d, now Prs=%v", r.id, id, r.Prs)
		if r.State == StateLeader {
			// node 减少，可能更容易提交了
			if r.maybeCommit() {
				// 如果 commitIndex 有更新，尝试同步
				r.bcastLogReplication()
			}
		}
	}
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
    r.campaign()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.campaign()
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
    r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
    r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
    r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
    r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
		// 已经在竞选了，不必再次发起选举
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		for pr := range r.Prs {
			if pr != r.id{
				r.sendHeartbeat(pr)
			}
		}
	case pb.MessageType_MsgPropose:
		if r.leadTransferee != None {
			return ErrProposalDropped
		}
		r.appendEntry(m.Entries...)
		r.bcastLogReplication()
	case pb.MessageType_MsgAppend:
		log.Panicf("split-brain! msg:%v, raft: %s", m, r.currentState())
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		log.Panicf("split-brain! msg:%v, raft: %s", m, r.currentState())
	case pb.MessageType_MsgHeartbeat:
		log.Panicf("split-brain! msg:%v, raft: %s", m, r.currentState())
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

// 超过半数的节点数量
func (r *Raft) quorum() int { return len(r.Prs)/2 + 1 }

// 参与竞选，发起投票，尝试成为 leader
func (r *Raft) campaign() {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
  r.becomeCandidate()

	// 如果集群中只有一个节点，那么直接当选
	if (len(r.Prs) == 1) {
		r.becomeLeader()
		return
	}

	// 向集群里的其他节点发送投票消息
	for id := range r.Prs {
		if id != r.id {
      r.sendRequestVote(id)
		}
	}
}

// 批量append一堆entries
func (r *Raft) appendEntry(es ...*pb.Entry) {
	li := r.RaftLog.LastIndex()
	for i := range es {
		// 设置这些entries的Term
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
		r.DPrintf(log.DPropose, "%d append an Entry(type=%d, index=%d) at term %d", r.id, es[i].EntryType, es[i].Index, r.Term)
	}
	r.RaftLog.append(es...)
	// 更新本节点的Next以及Match索引
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	// append之后，尝试一下是否可以进行commit
	if len(r.Prs) == 1{
		r.RaftLog.commitTo(r.Prs[r.id].Match)
	}
}

// send persists state to stable storage and then sends to its mailbox.
func (r *Raft) send(m pb.Message) {
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

// 向所有follower发送append消息
func (r *Raft) bcastLogReplication() {
	for id := range r.Prs {
		if id != r.id {
			r.replicateLog(id)
		}
	}
}

// 尝试commit当前的日志，如果commit日志索引发生变化了就返回true
func (r *Raft) maybeCommit() bool {
	mis := make(uint64Slice, 0, len(r.Prs))
	// 拿到当前所有节点的Match到数组中
	for id := range r.Prs {
		mis = append(mis, r.Prs[id].Match)
	}
	// 逆序排列
	sort.Sort(sort.Reverse(mis))
	// 排列之后拿到中位数的Match，因为如果这个位置的Match对应的Term也等于当前的Term
	// 说明有过半的节点至少comit了mci这个索引的数据，这样leader就可以以这个索引进行commit了
	mci := mis[r.quorum()-1]
	// raft日志尝试commit
  if mci > r.RaftLog.committed && mustTerm(r.RaftLog.Term(mci)) == r.Term {
		r.DPrintf(log.DCommit, "%d fit quorum, commit to %d", r.id, mci)
    r.RaftLog.commitTo(mci)
    return true
  }
  return false
}

func (r *Raft) loadState(state pb.HardState) {
	log.Assert(state.Commit >= r.RaftLog.committed && state.Commit <= r.RaftLog.LastIndex(),
		"%x state.commit %d is out of range [%d, %d]", r.id, state.Commit, r.RaftLog.committed, r.RaftLog.LastIndex())
	r.RaftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}

func (r *Raft) replicateLog(to uint64) {
	if !r.sendAppend(to) {
		r.sendSnapshot(to)
	}
}

// For debug
func (r *Raft) currentState() string {
	return fmt.Sprintf("State of %d: {term: %d, vote: %d, State: %v, Prs:%v, first: %d, applied: %d, committed: %d, stabled: %d, last: %d}", 
		r.id, r.Term, r.Vote, r.State, r.Prs, r.RaftLog.FirstIndex(), r.RaftLog.applied, r.RaftLog.committed, r.RaftLog.stabled, r.RaftLog.LastIndex())
}

func (r *Raft) DPrintf(topic log.LogTopic, format string, a ...interface{}) {
	log.DPrintf(topic, format + "\n" + r.currentState(), a...)
}