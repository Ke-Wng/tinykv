package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// 查找 entry 对应的 proposal
func (d *peerMsgHandler) findProposal(eindex, eterm uint64) *proposal {
	for len(d.proposals) > 0 {
		p := d.proposals[0]
		if p.term < eterm {
			// p 是当前 peer 之前任期内接收的提议，但是最终没有达成共识
			// 现在任期变化了, p 也就过期了
			if p.cb != nil {
				NotifyStaleReq(p.term, p.cb)
			}
			d.proposals = d.proposals[1:]
			continue
		}
		if p.term > eterm {
			// entry 不是通过当前 peer 提出的
			// p 是当前 peer 在 entry 后面的某个任期中接收的提议
			return nil
		}
		// p.term == eterm
		if p.index < eindex {
			// p, entry 都是当前 peer 接收的提议
			// p 没有达成共识，entry 达成共识了，说明 p 过期了
			if p.cb != nil {
				NotifyStaleReq(p.term, p.cb)
			}
			d.proposals = d.proposals[1:]
			continue
		} else if p.index == eindex {
			d.proposals = d.proposals[1:]
			return p
		} else {
      // 这是什么情况？
      return nil
    }
	}
	// 说明这个 entry 不是通过当前这个 leader 提出的
	return nil
}

func (d *peerMsgHandler) handleClientRequests(requests []*raft_cmdpb.Request, kvWB *engine_util.WriteBatch, cb *message.Callback) {
	// 检查所有 key 是否都在 region 中，同时执行或同时 abort
	for _, req := range requests {
		var key []byte
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			key = req.Put.Key
		case raft_cmdpb.CmdType_Get:
			key = req.Get.Key
		case raft_cmdpb.CmdType_Delete:
			key = req.Delete.Key
		}
		if req.CmdType != raft_cmdpb.CmdType_Snap {
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				// 更新 ApplyIndex
				kvWB.WriteToDB(d.ctx.engine.Kv)
				if cb != nil {
					cb.Done(ErrResp(err))
				}
				return
			}
		}
	}

	// 执行写操作
	for _, req := range requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
      // d.SizeDiffHint += uint64(len(req.Put.Key) + len(req.Put.Value))
		case raft_cmdpb.CmdType_Get:
		case raft_cmdpb.CmdType_Delete:
			kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
      // d.SizeDiffHint -= uint64(len(req.Delete.Key))
		case raft_cmdpb.CmdType_Snap:
		}
	}
  // 落盘
  kvWB.WriteToDB(d.ctx.engine.Kv)
  if cb == nil {
    return
  }

  // 响应 (落盘以后才能响应)
	// 一条 entry 可能包含多条操作，都已落盘，可以放心读取
	resp := &raft_cmdpb.RaftCmdResponse{
		Header:    &raft_cmdpb.RaftResponseHeader{},
		Responses: make([]*raft_cmdpb.Response, len(requests)),
	}
	for i, req := range requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			resp.Responses[i] = &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}}
		case raft_cmdpb.CmdType_Get:
			val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			resp.Responses[i] = &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: val}}
		case raft_cmdpb.CmdType_Delete:
			resp.Responses[i] = &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}}
		case raft_cmdpb.CmdType_Snap:
      // copy region
      region := new(metapb.Region)
      err := util.CloneMsg(d.Region(), region)
      if err != nil {
        panic(err)
      }
			resp.Responses[i] = &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: region}}
			cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
	}
	cb.Done(resp)
}

func (d *peerMsgHandler) handleCompactLog(request *raft_cmdpb.CompactLogRequest, kvWB *engine_util.WriteBatch) {
	compactIndex := request.CompactIndex
	compactTerm := request.CompactTerm
	if compactIndex >= d.peerStorage.applyState.TruncatedState.Index {
		d.peerStorage.applyState.TruncatedState.Index = compactIndex
		d.peerStorage.applyState.TruncatedState.Term = compactTerm
		if err := kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState); err != nil {
			panic(err)
		}
		kvWB.WriteToDB(d.ctx.engine.Kv)
		d.ScheduleCompactLog(compactIndex)
	} else {
    // 更新 applyIndex
		kvWB.WriteToDB(d.ctx.engine.Kv)
  }
}

func (d *peerMsgHandler) handleChangePeer(cc *eraftpb.ConfChange, kvWB *engine_util.WriteBatch, cb *message.Callback) {
  region := d.Region()
  msg := &raft_cmdpb.RaftCmdRequest{}
  msg.Unmarshal(cc.Context)
  resp := &raft_cmdpb.RaftCmdResponse{
    Header: &raft_cmdpb.RaftResponseHeader{},
    AdminResponse: &raft_cmdpb.AdminResponse{
      CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
      ChangePeer: &raft_cmdpb.ChangePeerResponse{},
    },
  }
  // 在 peer 层面修改配置
  switch cc.ChangeType {
  case eraftpb.ConfChangeType_AddNode:
    for _, p := range region.Peers {
      if p.Id == cc.NodeId {
				kvWB.WriteToDB(d.ctx.engine.Kv)
        return
      }
    }
    newPeer := &metapb.Peer{
      Id:      cc.NodeId,
      StoreId: msg.AdminRequest.ChangePeer.Peer.StoreId,
    }
    region.Peers = append(region.Peers, newPeer)
    region.RegionEpoch.ConfVer++
    d.insertPeerCache(newPeer)
		meta := d.ctx.storeMeta
		meta.Lock()
		meta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		meta.Unlock()
  case eraftpb.ConfChangeType_RemoveNode:
    if cc.NodeId == d.PeerId() {
			kvWB.WriteToDB(d.ctx.engine.Kv)
      d.destroyPeer()
      if cb != nil {
        cb.Done(resp)
      }
      d.removePeerCache(cc.NodeId)
      // 不能再往下执行了
      return
    }
    for i, pr := range region.Peers {
      if pr.Id == cc.NodeId {
        region.Peers = append(region.Peers[:i], region.Peers[i+1:]...)
        region.RegionEpoch.ConfVer++
        d.removePeerCache(cc.NodeId)
        break
      }
    }
  }
	d.notifyHeartbeatScheduler(d.Region(), d.peer)
  log.SDebug("%s after ConfChange, region=%v", d.Tag, region)
  meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
  kvWB.WriteToDB(d.peerStorage.Engines.Kv)
  // 在 Raft 层面修改配置
  d.RaftGroup.ApplyConfChange(*cc)
  // 响应
  if cb != nil {
    // 说明这个 proposal 是这个 peer 接收的，并且达成共识，需要响应
    cb.Done(resp)
  }
}

func (d *peerMsgHandler) handleSplitRequest(request *raft_cmdpb.SplitRequest, kvWB *engine_util.WriteBatch, cb *message.Callback) {
	if err := util.CheckKeyInRegion(request.SplitKey, d.Region()); err != nil {
		// 更新 ApplyIndex
		kvWB.WriteToDB(d.ctx.engine.Kv)
		if cb != nil {
			cb.Done(ErrResp(err))
		}
		return
	}
	if len(request.NewPeerIds) != len(d.Region().Peers) {
		// 更新 ApplyIndex
		kvWB.WriteToDB(d.ctx.engine.Kv)
		if cb != nil {
			cb.Done(ErrResp(&util.ErrStaleCommand{}))
		}
		return
	}

  region := d.Region()
  newRegion := &metapb.Region{}
  util.CloneMsg(region, newRegion)

  region.RegionEpoch.Version++
  region.EndKey = request.SplitKey
  meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
  newRegion.RegionEpoch.Version++
  newRegion.Id = request.NewRegionId
  newRegion.StartKey = request.SplitKey
	for i, id := range request.NewPeerIds {
		newRegion.Peers[i].Id = id
	}
  meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
  kvWB.WriteToDB(d.ctx.engine.Kv)

  // clear region size
  d.SizeDiffHint = 0
  d.ApproximateSize = new(uint64)

  newPeer, _ := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
  d.ctx.router.register(newPeer)
	_ = d.ctx.router.send(newPeer.regionId, message.Msg{Type: message.MsgTypeStart, RegionID: newPeer.regionId})

  meta := d.ctx.storeMeta
  meta.Lock()
  meta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
  meta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	meta.setRegion(region, d.peer)
	meta.setRegion(newRegion, newPeer)
  meta.Unlock()

	d.notifyHeartbeatScheduler(region, d.peer)
	d.notifyHeartbeatScheduler(newRegion, newPeer)

  if cb != nil {
    cb.Done(&raft_cmdpb.RaftCmdResponse{
      Header: &raft_cmdpb.RaftResponseHeader{},
      AdminResponse: &raft_cmdpb.AdminResponse{
        CmdType: raft_cmdpb.AdminCmdType_Split,
        Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{region, newRegion}},
      },
    })
  }
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
  clonedRegion := new(metapb.Region)
  err := util.CloneMsg(region, clonedRegion)
  if err != nil {
    return
  }
  d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
    Region:          clonedRegion,
    Peer:            peer.Meta,
    PendingPeers:    peer.CollectPendingPeers(),
    ApproximateSize: peer.ApproximateSize,
  }
}

func (d *peerMsgHandler) handleReadRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
  if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
    if cb != nil {
      cb.Done(ErrResp(err))
    }
    return
  }

	for _, req := range msg.Requests {
		if req.CmdType == raft_cmdpb.CmdType_Get {
			if err := util.CheckKeyInRegion(req.Get.Key, d.Region()); err != nil {
				if cb != nil {
					cb.Done(ErrResp(err))
				}
				return
			}
		}
	}

  resp := &raft_cmdpb.RaftCmdResponse{
    Header:    &raft_cmdpb.RaftResponseHeader{},
    Responses: make([]*raft_cmdpb.Response, len(msg.Requests)),
  }
	for i, req := range msg.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			resp.Responses[i] = &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: val}}
		case raft_cmdpb.CmdType_Snap:
			// copy region
			region := new(metapb.Region)
			err := util.CloneMsg(d.Region(), region)
			if err != nil {
				panic(err)
			}
			resp.Responses[i] = &raft_cmdpb.Response{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: region}}
			cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
	}
  cb.Done(resp)
}

func (d *peerMsgHandler) handleLeaseRead(committedIndex uint64) {
	for len(d.pendingReads) > 0 {
    readCmd := d.pendingReads[0]
		cb := readCmd.cb
    if readCmd.term < d.Term() {
      if cb != nil {
        cb.Done(ErrResp(&util.ErrStaleCommand{}))
      }
			d.pendingReads = d.pendingReads[1:]
      continue
    }

    log.Assert(readCmd.term == d.Term(), "pengding read's term=%d can not larger than current term=%d", readCmd.term, d.Term())

		// readIndex 应该是递增的
		if committedIndex < readCmd.readIndex {
			return
		}

    log.SDebug("%s handle lease read at readIndex=%d, term=%d", d.Tag, readCmd.readIndex, readCmd.term)
    d.pendingReads = d.pendingReads[1:]
    d.handleReadRequest(readCmd.msg, cb)
	}
}

func (d *peerMsgHandler) applyLogEntriesAndSnapshot(entries []eraftpb.Entry, snapshot *eraftpb.Snapshot) {
	if d.stopped {
		return
	}

	if snapshot != nil && !raft.IsEmptySnap(snapshot) {
		applyResult, err := d.peerStorage.ApplySnapshot(snapshot)
		if err != nil {
			panic(err)
		}
		// 如果应用了快照，region 信息可能发生变更
		// ApplySnapshot() 已经将变化持久化，这里还需要将变化写入 storeMeta 和 peer
		if applyResult != nil {
			meta := d.ctx.storeMeta
			meta.Lock()
			delete(meta.regions, applyResult.PrevRegion.Id)
			meta.regionRanges.Delete(&regionItem{region: applyResult.PrevRegion})
			// 连带 storeMeta 和 peer 的 region 一起更新
			meta.setRegion(applyResult.Region, d.peer)
			meta.regionRanges.ReplaceOrInsert(&regionItem{region: applyResult.Region})
			meta.Unlock()
			d.RaftGroup.SnapshotAdvance()
		}
	}

	// 应用日志到状态机
	applyState := d.peerStorage.applyState
	for _, entry := range entries {
		if entry.Index <= applyState.AppliedIndex {
			// 忽略重复发送的 entries
			continue
		}
		if applyState.TruncatedState.Index >= entry.Index {
			// 刚应用完快照，跳过过期的 entry
			continue
		}
		// 跳过空日志
		if entry.Data == nil {
			kvWB := &engine_util.WriteBatch{}
			log.Assert(applyState.AppliedIndex + 1 == entry.Index, "%s out of order apply: appliedIndex=%d, entryIndex=%d", d.Tag, applyState.AppliedIndex, entry.Index)
			applyState.AppliedIndex = entry.Index
			kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
			kvWB.WriteToDB(d.ctx.engine.Kv)
			d.RaftGroup.ApplyAdvance(applyState.AppliedIndex)
      d.handleLeaseRead(applyState.AppliedIndex)
			continue
		}

    p := d.findProposal(entry.Index, entry.Term)
    var cb *message.Callback
    if p != nil {
      cb = p.cb
    }
    kvWB := &engine_util.WriteBatch{}
		log.Assert(applyState.AppliedIndex + 1 == entry.Index, "%s out of order apply: appliedIndex=%d, entryIndex=%d", d.Tag, applyState.AppliedIndex, entry.Index)
    applyState.AppliedIndex = entry.Index
    kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)

		// 执行
		switch entry.EntryType {
		case eraftpb.EntryType_EntryNormal:
			msg := &raft_cmdpb.RaftCmdRequest{}
			msg.Unmarshal(entry.Data)
			// 不执行过期的请求
			if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
				p := d.findProposal(entry.Index, entry.Term)
				if p != nil {
					p.cb.Done(ErrResp(err))
				}
				log.SDebug("%s ignore stale NormalEntry at index=%d, term=%d", d.Tag, entry.Index, entry.Term)
        // 更新 applyIndex
        kvWB.WriteToDB(d.ctx.engine.Kv)
				break
			}
			if msg.AdminRequest != nil {
				adminReq := msg.AdminRequest
				switch adminReq.CmdType {
				case raft_cmdpb.AdminCmdType_CompactLog:
					log.SDebug("%s apply CompactLog=%v at index=%d, term=%d", d.Tag, *(adminReq.CompactLog), entry.Index, entry.Term)
          d.handleCompactLog(adminReq.CompactLog, kvWB)
        case raft_cmdpb.AdminCmdType_Split:
					log.SDebug("%s apply Split={regionId:%v, peers:%v, splitkey:%v} at index=%d, term=%d", d.Tag, adminReq.Split.NewRegionId, adminReq.Split.NewPeerIds, adminReq.Split.SplitKey, entry.Index, entry.Term)
          d.handleSplitRequest(msg.AdminRequest.Split, kvWB, cb)
				}
			} else {
				log.SDebug("%s apply client command at index=%d, term=%d", d.Tag, entry.Index, entry.Term)
				d.handleClientRequests(msg.Requests, kvWB, cb)
			}
		case eraftpb.EntryType_EntryConfChange:
			cc := &eraftpb.ConfChange{}
			cc.Unmarshal(entry.Data)
			msg := &raft_cmdpb.RaftCmdRequest{}
			msg.Unmarshal(cc.Context)
			log.SDebug("%s apply ConfChange=%v at index=%d, term=%d", d.Tag, *cc, entry.Index, entry.Term)
			// 不执行过期的请求
			if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
				p := d.findProposal(entry.Index, entry.Term)
				if p != nil {
					p.cb.Done(ErrResp(err))
				}
				log.SDebug("%s ignore stale NormalEntry at index=%d, term=%d", d.Tag, entry.Index, entry.Term)
        // 更新 applyIndex
        kvWB.WriteToDB(d.ctx.engine.Kv)
				break
			}
      d.handleChangePeer(cc, kvWB, cb)
		}

    if d.stopped {
      return
    }

		d.RaftGroup.ApplyAdvance(applyState.AppliedIndex)
    d.handleLeaseRead(applyState.AppliedIndex)
	}
}


func (d *peerMsgHandler) HandleRaftReady(applyPool *RaftLogApplyPool) {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	// 获取 Ready
	if !d.RaftGroup.HasReady() {
		return
	}
	rd := d.RaftGroup.Ready()

	applyPool.Submit(&RaftLogApplyTask{
		entries: rd.CommittedEntries,
		snapshot: &rd.Snapshot,
		peer: d.peer,
		ctx: d.ctx,
	})
	
	// 持久化日志和 HardState
	err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}

	// 发送网络消息
	if len(rd.Messages) != 0 {
		d.Send(d.ctx.trans, rd.Messages)
	}

	d.RaftGroup.PeerAdvance(rd)
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) propose(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	proposal := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}
	data, err := msg.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	err = d.RaftGroup.Propose(data)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	d.proposals = append(d.proposals, proposal)
	if len(msg.Requests) > 0 {
		log.SDebug("%s propose client command at index=%d, term=%d", d.Tag, proposal.index, proposal.term)
	} else {
		log.SDebug("%s propose %v command at index=%d, term=%d", d.Tag, msg.AdminRequest.CmdType, proposal.index, proposal.term)
	}
}

func (d *peerMsgHandler) proposeLeaseRead(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) bool {
	if committedIndex, ok := d.RaftGroup.InLease(); ok {
		if d.peerStorage.applyState.AppliedIndex == committedIndex {
			log.SDebug("%s is in lease and appliedIndex=committedIndex, perform read directly, readIndex=%d, term=%d", d.Tag, committedIndex, d.Term())
			d.handleReadRequest(msg, cb)
		} else {
			log.SDebug("%s is in lease, propose lease read at readIndex=%d, term=%d", d.Tag, committedIndex, d.Term())
			d.pendingReads = append(d.pendingReads, &readCmd{
				readIndex: committedIndex,
				term: d.Term(),
				msg: msg,
				cb: cb,
			})
		}
		return true
	}
	return false	
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.Raft.State != raft.StateLeader {
		cb.Done(ErrResp(&util.ErrNotLeader{
			RegionId: d.regionId,
		}))
		return
	}
	if err := util.CheckPeerID(msg, d.PeerId()); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	if err := util.CheckRegionEpoch(msg, d.Region(), false); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	if len(msg.Requests) > 0 {
		// 检查所有 key 是否都在 region 中
		readOnly := true
		for _, req := range msg.Requests {
			var key []byte
			switch req.CmdType {
			case raft_cmdpb.CmdType_Put:
				key = req.Put.Key
				readOnly = false
			case raft_cmdpb.CmdType_Get:
				key = req.Get.Key
			case raft_cmdpb.CmdType_Delete:
				key = req.Delete.Key
				readOnly = false
			}
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil && req.CmdType != raft_cmdpb.CmdType_Snap {
				cb.Done(ErrResp(err))
				return
			}
		}
		// 如果是 read only 请求，尝试 lease read
		if readOnly {
			if d.proposeLeaseRead(msg, cb) {
				return
			}
		}
		d.propose(msg, cb)
	} else if msg.AdminRequest != nil {
		req := msg.AdminRequest
		switch req.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.propose(msg, cb)
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.RaftGroup.TransferLeader(req.TransferLeader.Peer.Id)
			resp := &raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}
			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			cb.Done(resp)
			log.SDebug("%s propose TranferLeader to %d", d.Tag, req.TransferLeader.Peer.Id)
		case raft_cmdpb.AdminCmdType_ChangePeer:
			proposal := &proposal{
				index: d.nextProposalIndex(),
				term:  d.Term(),
				cb:    cb,
			}
			data, err := msg.Marshal()
			if err != nil {
				log.Panic(err)
			}
			cc := eraftpb.ConfChange{
				ChangeType: req.ChangePeer.ChangeType,
				NodeId:     req.ChangePeer.Peer.Id,
				Context:    data,
			}
      // 当集群中只剩下两个节点，并且要删除的是 leader
      // 拒绝请求，并发起 leader tranfer
      if len(d.Region().Peers) == 2 && cc.ChangeType == eraftpb.ConfChangeType_RemoveNode && cc.NodeId == d.PeerId() {
        for _, pr := range d.Region().Peers {
          if pr.Id != d.PeerId() {
            d.RaftGroup.TransferLeader(pr.Id)
            cb.Done(ErrResp(&util.ErrStaleCommand{})) // 让客户端重试
          }
        }
        return
      }
			if err := d.RaftGroup.ProposeConfChange(cc); err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.proposals = append(d.proposals, proposal)
			log.SDebug("%s propose ConfChange=%v at index=%d, term=%d", d.Tag, cc, proposal.index, proposal.term)
		case raft_cmdpb.AdminCmdType_Split:
      // check splitKey
      if err := util.CheckKeyInRegion(req.Split.SplitKey, d.Region()); err != nil {
        cb.Done(ErrResp(err))
        return
      }
      d.propose(msg, cb)
		}
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
