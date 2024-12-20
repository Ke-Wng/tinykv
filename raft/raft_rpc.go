package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

//
// Heartbeat
//

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term: r.Term,
		Commit: commit,
		To: to,
		From: r.id,
	}
  r.send(msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.Lead = m.From
	r.Term = m.Term
	if (m.Commit > r.RaftLog.committed) {
		r.DPrintf(log.DCommit, "%d catch up leader's commit to %d", r.id, m.Commit)
	}
	r.RaftLog.commitTo(m.Commit)
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	log.Assert(r.State == StateLeader, "%s can not handle HeartBeatResponse", r.State)
	pr := r.Prs[m.From]
	// 节点的日志滞后了，尝试同步
	if pr.Match < r.RaftLog.LastIndex() {
		r.replicateLog(m.From)
	}
}


//
// Vote
//

// sendRequestVote sends a RequestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	msg := pb.Message{
		Term: r.Term,
		To: to,
		MsgType: pb.MessageType_MsgRequestVote,
		Index: r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
	}

	r.send(msg)
}

func (r *Raft) handleRequestVote(m pb.Message) {
  lasti := r.RaftLog.LastIndex()
  lastt := r.RaftLog.LastTerm()
  if m.Term >= r.Term &&
    ((r.Vote == None) || (r.Vote == m.From)) && 
    (m.LogTerm > lastt || (m.LogTerm == lastt && m.Index >= lasti)) {
		r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
		// 保存下来给哪个节点投票了
		r.electionElapsed = 0
		r.Vote = m.From
		r.DPrintf(log.DVote, "%d vote for %d", r.id, m.From)
  } else {
		// 拒绝投票
		r.DPrintf(log.DVote, "%d refused vote for %d", r.id, m.From)
		r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
  }
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	log.Assert(r.State == StateCandidate, "%s can not handle RequestVoteResponse", r.State)
	agrNum	:= 0 // 赞同票数
	denNum := 0// 反对票数
	r.votes[m.From] = !m.Reject
	for _, vote := range r.votes {
		if vote {
			agrNum ++
		} else {
			denNum ++
		}
	}
	if agrNum >= r.quorum() {
		r.becomeLeader()
	} else if denNum >= r.quorum() {
		r.becomeFollower(r.Term, None)
	}
}


//
// Append
//

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	log.Assert(r.State == StateLeader, "%s can not send AppendEntries", r.State)
	pr, ok := r.Prs[to]
	if !ok {
		panic("")
	}
	prevLogIndex := pr.Next - 1
	term := r.Term
	leaderId := r.id
	committedIndex := r.RaftLog.committed
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)

  if err == ErrCompacted {
		// 日志落后太多，需要发送快照
    return false
  } else if err != nil {
		panic(err)
	}
	// 从 nextIndex 开始发送
	firstIndex := r.RaftLog.FirstIndex()
	var entries []*pb.Entry
	for i := pr.Next; i <= r.RaftLog.LastIndex(); i++ {
		entries = append(entries, &r.RaftLog.entries[i-firstIndex])
	}

	// pipeline
	r.Prs[to].Next = r.RaftLog.LastIndex() + 1

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To: to,
		From: leaderId,
		Term: term,
		LogTerm: prevLogTerm,
		Index: prevLogIndex,
		Entries: entries,
		Commit: committedIndex,
	}

	r.DPrintf(log.DAppend, "%d append entries[%d:] to %d at term %d", r.id, pr.Next, to, r.Term)
  r.send(msg)
	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	log.Assert(r.State == StateFollower, "%s can not handle AppendEntries", r.State)
	r.Lead = m.From

	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Term: r.Term,
		To: m.From,
		From: r.id,
		// 成功时，返回接收者最新同步到的索引
		// 失败时，帮助 leader 调整 nextIndex
		Index: r.RaftLog.committed,
	}
	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm

	// 如果接收者的日志在 prevLogIndex 处没有 prevLogTerm 这个任期的条目（一致性检查失败）
	if term, err := r.RaftLog.Term(prevLogIndex); err != nil {
		// 如果日志被 compact 了，应该返回成功
		if err == ErrCompacted {
			r.DPrintf(log.DAppend, "%d accept compacted entries[%d:] from %d", r.id, prevLogIndex+1, m.From)
		} else {
			resp.Reject = true
			r.DPrintf(log.DAppend, "%d refuse conflicted entries from %d", r.id, m.From)
		}
		r.send(resp)
		return
	} else if term != prevLogTerm {
		resp.Reject = true
		resp.LogTerm = term // ConflictTerm
		for i := range r.RaftLog.entries {
			if r.RaftLog.entries[i].Term == term {
				resp.Index = r.RaftLog.entries[i].Index
				break
			}
		}
		r.send(resp)
		r.DPrintf(log.DAppend, "%d refuse conflicted entries from %d", r.id, m.From)
		return
	}

	r.DPrintf(log.DAppend, "%d accept entries[%d:] from %d", r.id, prevLogIndex+1, m.From)

	// 追加新条目，同时删除冲突
	for _, en := range m.Entries {
		index := en.Index
		oldTerm, _ := r.RaftLog.Term(index)
		if index - r.RaftLog.FirstIndex() > uint64(len(r.RaftLog.entries)) || index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries,*en)
		} else if oldTerm != en.Term {
			// 不匹配，删除从此往后的所有条目
			if index < r.RaftLog.FirstIndex() {
				r.RaftLog.entries = make([]pb.Entry , 0)
			}else {
				r.RaftLog.entries = r.RaftLog.entries[0 : index - r.RaftLog.FirstIndex()]
			}
			// 更新stable
			r.RaftLog.stabled = min(r.RaftLog.stabled, index - 1)
			// 追加新条目
			r.RaftLog.entries = append(r.RaftLog.entries,*en)
		}
	}

	// 传入数据的最后一条索引
	lastnewi := m.Index + uint64(len(m.Entries))
	resp.Index = lastnewi
	// 更新commitIndex
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.commitTo(min(m.Commit, lastnewi))
		r.DPrintf(log.DCommit, "%d catch up leader's commit to %d", r.id, min(m.Commit, lastnewi))
	}
	r.send(resp)
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	log.Assert(r.State == StateLeader, "%s can not handle AppendResponse", r.State)

	// 同步失败，更新 next 重新同步（一次跳过一条日志）
	if m.Reject {
		if m.LogTerm != 0 {
			found := false
			for i := range r.RaftLog.entries {
				if r.RaftLog.entries[i].Term == m.LogTerm {
					r.Prs[m.From].Next = r.RaftLog.entries[i].Index + 1
					found = true
					break
				}
			}
			if !found {
				r.Prs[m.From].Next = m.Index
			}
		} else {
			r.Prs[m.From].Next = m.Index + 1
		}
		r.replicateLog(m.From)
		return
	}

	// 同步成功, 更新 match 和 next
	r.Prs[m.From].Match = max(r.Prs[m.From].Match, m.Index) // match 不能回退
	r.Prs[m.From].Next = m.Index + 1 // next 回退也问题不大，最多多发送几条 entry

	// 更新 commit
	if r.maybeCommit() {
		// 如果 commitIndex 有更新，尝试同步
		r.bcastLogReplication()
	}

	// Transfer leadership is in progress.
	if m.From == r.leadTransferee && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	}
}


//
// Snapshot
//

func (r *Raft) sendSnapshot(to uint64) {
	var snapshot pb.Snapshot
	if r.RaftLog.pendingSnapshot == nil {
		var err error
		snapshot, err = r.RaftLog.storage.Snapshot()
		if err != nil {
			r.DPrintf(log.DSnap, "%d fail to send snapshot to %d at term %d, because %v", r.id, to, r.Term, err)
			return
		}
	} else {
		snapshot = *r.RaftLog.pendingSnapshot
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgSnapshot,
		To: to,
		From: r.id,
		Term: r.Term,
		Snapshot: &snapshot,
	}
  r.send(msg)
	r.DPrintf(log.DSnap, "%d send snapshot to %d at term %d", r.id, to, r.Term)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	// 发送响应消息
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Term: r.Term,
		To: m.From,
		From: r.id,
		Index: r.RaftLog.committed,
	}
	r.Lead = m.From
	meta := m.Snapshot.Metadata
	// stale snapshot，日志不能回退
	if meta.Index <= r.RaftLog.committed {
    resp.Index = r.RaftLog.LastIndex()
    resp.Reject = true
    r.send(resp)
		r.DPrintf(log.DSnap, "%d refuse stale snapshot(index=%d, term=%d) from %d", r.id, meta.Index, meta.Term, m.From)
		return
	}
	r.RaftLog.installSnapshot(m.Snapshot)
	// 集群成员变更
	if meta.ConfState != nil {
		r.Prs = make(map[uint64]*Progress)
		for _, node := range meta.ConfState.Nodes {
			r.Prs[node] = &Progress{Next: r.RaftLog.LastIndex()+1}
		}
	}
	resp.Index = meta.Index
	r.send(resp)
	r.DPrintf(log.DSnap, "%d install snapshot(index=%d, term=%d) from %d", r.id, meta.Index, meta.Term, m.From)
}


//
// Leader transfer
//

func (r *Raft) handleTransferLeader(m pb.Message) {
	leadTransferee := m.From
	lastLeadTransferee := r.leadTransferee
	if _, ok := r.Prs[leadTransferee]; !ok {
		return
	}
	// 如果之前有 transfer 正在进行
	if lastLeadTransferee != None {
		if lastLeadTransferee == leadTransferee {
			// 转移目标不变，直接返回
			return
		}
		// 要更换转移目标，中断之前的转让流程
		r.leadTransferee = None
	}
	// 判断是否转让过来的leader是否本节点，如果是也直接返回，因为本节点已经是leader了
	if leadTransferee == r.id {
		return
	}
	// 开始进行 transfer
	r.electionElapsed = 0 // Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
	r.leadTransferee = leadTransferee
	if r.Prs[leadTransferee].Match == r.RaftLog.LastIndex() {
		// transferee 的日志已经是最新的了
		r.sendTimeoutNow(leadTransferee)
	} else {
		// 同步日志
		r.replicateLog(leadTransferee)
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To: to, MsgType: pb.MessageType_MsgTimeoutNow})
}
