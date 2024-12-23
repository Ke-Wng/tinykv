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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	log.Assert(storage != nil, "storage must not be nil")
	log := &RaftLog{
		storage: storage,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err) // TODO(bdarnell)
	}

	log.stabled = lastIndex
	log.entries, _ = storage.Entries(firstIndex, lastIndex+1)
	// committed和applied从持久化的第一个index的前一个开始
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	if len(l.entries) == 0 {
		return
	}
	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		return
	}
	if firstIndex > l.entries[0].Index {
		// storage 层发生了 compaction，丢弃被 compact 的日志
		if firstIndex > l.LastIndex() {
			l.entries = make([]pb.Entry, 0)
		} else {
			l.entries = l.entries[firstIndex-l.entries[0].Index:]
		}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if (len(l.entries) == 0) {
		return make([]pb.Entry, 0)
	}
	firstIndex := l.FirstIndex()
	if l.stabled < firstIndex {
		return l.entries
	}
	if l.stabled-firstIndex >= uint64(len(l.entries)-1) {
		return make([]pb.Entry, 0)
	}
	return l.entries[l.stabled-firstIndex+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	firstIndex := l.FirstIndex()
	off := max(l.applied+1, firstIndex)
	if l.committed+1 > off {	// 如果commit索引比前面得到的值还大，说明还有没有commit了但是还没apply的数据，将这些数据返回
    return l.entries[off-firstIndex:l.committed+1-firstIndex]
	}
	return make([]pb.Entry, 0)
}

// 返回 fistIndex (没有被 compact 的第一个索引)
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index 
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	index, _ := l.storage.FirstIndex()
	return index
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) != 0 {
		return l.entries[len(l.entries)-1].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	// 说明刚进行了 compact，数据在 storage
	index, _ := l.storage.LastIndex()
	return index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
  firstIndex := l.FirstIndex()
	if i < firstIndex - 1 {
		return 0, ErrCompacted
	} else if i > l.LastIndex() {
		return 0, ErrUnavailable
	}

  // 如果要找的是 truncatedTerm
	if l.pendingSnapshot != nil && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	if len(l.entries) == 0 || i == firstIndex - 1 {
    return l.storage.Term(i)
	}

	return l.entries[i-firstIndex].Term, nil
}

func (l *RaftLog) LastTerm() uint64 {
	// Your Code Here (2A).
	return mustTerm(l.Term(l.LastIndex()))
}

// 将raftlog的commit索引，修改为tocommit
func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	// 首先需要判断，commit索引绝不能变小
	if tocommit <= l.committed {
		return
	}
	l.committed = tocommit
}

// 修改applied索引
func (l *RaftLog) appliedTo(i uint64) {
	if i <= l.applied {
		return
	}
	// 判断合法性
	// 新的applied ID不能比committed大
	log.Assert(i <= l.committed,
		"applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	l.applied = i
}

// 将entries附加到日志最后
func (l *RaftLog) append(ents ...*pb.Entry) {
	li := l.LastIndex()
	for i, e := range(ents) {
		e.Index = li + 1 + uint64(i)
		l.entries = append(l.entries, *e)
	}
}

func (l *RaftLog) installSnapshot(snapshot *pb.Snapshot) {
	index, term := snapshot.Metadata.Index, snapshot.Metadata.Term
	if index < l.LastIndex() && mustTerm(l.Term(index)) == term {
		// there is an entry at lastIncludedIndex with the same term
		// only discard entries after lastIncludedIndex
		// 丢弃 index 及其之前的 entry
		l.entries = l.entries[index-l.entries[0].Index+1:]
	} else {
		// discard all log
		l.entries = make([]pb.Entry, 0)
	}
	l.commitTo(index)
	l.appliedTo(index)
	l.stabled = max(l.stabled, index)
	l.pendingSnapshot = snapshot
}
