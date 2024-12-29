package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)


type RaftLogApplyTask struct {
	entries					[]eraftpb.Entry
	snapshot				*eraftpb.Snapshot
	peer						*peer
	ctx							*GlobalContext
}

type RaftLogApplyPool struct {
	workerChs  	[]chan *RaftLogApplyTask
	closeCh			chan struct{}
	wg      		sync.WaitGroup
}

func NewRaftLogApplyPool(nworker int) *RaftLogApplyPool {
	workerChs := make([]chan *RaftLogApplyTask, nworker)
	for i := range workerChs {
		workerChs[i] = make(chan *RaftLogApplyTask)
	}

	pool := &RaftLogApplyPool{
		workerChs: workerChs,
		closeCh: make(chan struct{}),
	}

	// Start workers
	for i := 0; i < nworker; i++ {
		pool.wg.Add(1)
		go pool.applyWorker(pool.workerChs[i])
	}

	return pool
}

func (p *RaftLogApplyPool) applyWorker(taskCh chan *RaftLogApplyTask) {
	defer p.wg.Done()
	for {
		select {
		case <-p.closeCh:
			// closeCh closed, drain remaining tasks and exit worker
			for task := range taskCh {
				p.work(task)
			}
			return
		case task, ok := <-taskCh:
			if !ok {
				return
			}
			p.work(task)
		}
	}
}

func (p *RaftLogApplyPool) work(task *RaftLogApplyTask) {
	entries := task.entries
	// n := len(entries)
	// log.SDebug("%s apply channel received entries[%d:%d]", task.peer.Tag, entries[0].Index, entries[n-1].Index)
	// log.SDebug("%s apply channel received snapshot=%v", task.peer.Tag, task.snapshot.Metadata)
	newPeerMsgHandler(task.peer, task.ctx).applyLogEntriesAndSnapshot(entries, task.snapshot)
}

// Submit adds a new task to the pool
func (p *RaftLogApplyPool) Submit(task *RaftLogApplyTask) {
	workerIndex := int(task.peer.regionId) % len(p.workerChs)
	p.workerChs[workerIndex] <- task
}

func (p *RaftLogApplyPool) Shutdown() {
	close(p.closeCh)
	for _, ch := range p.workerChs {
		close(ch)
	}
	p.wg.Wait()
}

