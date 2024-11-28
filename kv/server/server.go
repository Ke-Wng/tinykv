package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
  defer reader.Close()

	lock, err := txn.GetLock(req.Key)
	if err != nil {
		return resp, err
	}

	if lock != nil && lock.Ts <= req.Version {
	// there is another txn writing the key
	// KvGet may not read the lastest value
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			}}
		return resp, err
	}

	value, err := txn.GetValue(req.Key)
	if err != nil {
		return resp, err
	}
	if value == nil {
		resp.NotFound = true
	}
	resp.Value = value

	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	userKeys := make([][]byte, 0)
	for _, mut := range req.Mutations {
		userKeys = append(userKeys, mut.Key)
	}
	server.Latches.WaitForLatches(userKeys)
	defer server.Latches.ReleaseLatches(userKeys)

	resp := &kvrpcpb.PrewriteResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
  defer reader.Close()

	// check W-W conflict
	for _, key := range userKeys {
		write, commitTs, err := txn.MostRecentWrite(key)
		if err != nil {
			return resp, err
		}
		if write != nil && commitTs > req.StartVersion {
			// W-W conflict	
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs: write.StartTS,
					ConflictTs: commitTs,
					Key: key,
					Primary: req.PrimaryLock,
				},
			})	
		}
	}
	if len(resp.Errors) != 0 {
		return resp, nil
	}

	// check other in-progress txns
	for _, key := range userKeys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, nil
		}
		if lock != nil && lock.Ts != req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: req.PrimaryLock,
					LockVersion: lock.Ts,
					Key: key,
					LockTtl: lock.Ttl,
				},
			})	
		}
	}
	if len(resp.Errors) != 0 {
		return resp, nil
	}

	// put values and hold locks
	for _, mut := range req.Mutations {
		var kind mvcc.WriteKind
		switch mut.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mut.Key, mut.Value)
			kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mut.Key)
			kind = mvcc.WriteKindDelete
		case kvrpcpb.Op_Rollback:
			kind = mvcc.WriteKindRollback
		case kvrpcpb.Op_Lock:
		}

		txn.PutLock(mut.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts: req.StartVersion,
			Ttl: req.LockTtl,
			Kind: kind,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	resp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
  defer reader.Close()

	// validate locks
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}

		if lock == nil {
			// check if it is rolled back
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return resp, err
			}
			if write != nil && write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "request has been rolled back",
				}
				return resp, nil
			}
			// re-commit or never prewrite
			return resp, nil
		} else if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "request has been rolled back or committed",
			}
			return resp, nil
		}

		// make writes available for later requests
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind: lock.Kind,
		})
		txn.DeleteLock(key)
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.Version)
  defer reader.Close()

  sIter := mvcc.NewScanner(req.StartKey, txn)
  defer sIter.Close()
  for len(resp.Pairs) < int(req.Limit) {
    key, value, err := sIter.Next()
    if err != nil {
			return resp, nil
    }
    if key == nil {
      break
    }
    if value == nil {
      continue
    }
		pair := &kvrpcpb.KvPair{
			Key: key,
			Value: value,
		}
		// check lock on the key
		lock, err := txn.GetLock(key)
    if err != nil {
			return resp, nil
    }
		if lock != nil && lock.Ts < req.Version {
			// key is locked by another txn
			pair.Error = &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lock.Primary,
					LockVersion: lock.Ts,
					Key:         key,
					LockTtl:     lock.Ttl,
				},
			}
		}
    resp.Pairs = append(resp.Pairs, pair)
  }

	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	resp.Action = kvrpcpb.Action_NoAction
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
  defer reader.Close()

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, nil
	}

  if lock != nil && lock.Ts == req.LockTs {
    // check timeout
    if mvcc.PhysicalTime(lock.Ts) + lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
			// roll back
      txn.DeleteLock(req.PrimaryKey)
      txn.DeleteValue(req.PrimaryKey)
      txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
        StartTS: req.LockTs,
        Kind:    mvcc.WriteKindRollback,
      })
      resp.Action = kvrpcpb.Action_TTLExpireRollback
      err = server.storage.Write(req.Context, txn.Writes())
      if err != nil {
        return resp, err
      }
    } else {
			// txn is locked
      resp.LockTtl = lock.Ttl
    }
    return resp, nil
  }

  // check if it is rolled back or commited
  write, commitTS, err := txn.CurrentWrite(req.PrimaryKey)
  if err != nil {
    return resp, err
  }
  if write == nil {
    // no data for the key
    resp.Action = kvrpcpb.Action_LockNotExistRollback
    txn.DeleteValue(req.PrimaryKey)
    txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
      StartTS: req.LockTs,
      Kind:    mvcc.WriteKindRollback,
    })
    err = server.storage.Write(req.Context, txn.Writes())
    if err != nil {
      return resp, err
    }
  } else if write.Kind == mvcc.WriteKindRollback {
    // already be rolled back
    resp.Action = kvrpcpb.Action_NoAction
  } else {
    // committed
    resp.CommitVersion = commitTS
  }
  return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)

	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock != nil && lock.Ts == req.StartVersion {
			txn.DeleteLock(key)
			txn.DeleteValue(key)
      txn.PutWrite(key, req.StartVersion, &mvcc.Write{
        StartTS: req.StartVersion,
        Kind:    mvcc.WriteKindRollback,
      })
		} else {
			write, _, err := txn.CurrentWrite(key)
			if err != nil {
				return resp, err
			}
			if write == nil {
				txn.PutWrite(key, req.StartVersion, &mvcc.Write{
					StartTS: req.StartVersion,
					Kind:    mvcc.WriteKindRollback,
				})
			} else if write.Kind != mvcc.WriteKindRollback {
				// already committed, can not be rolled back
				resp.Error = &kvrpcpb.KeyError{
					Abort: "true",
				}
			}
			// skip if it is already rolled back
		}
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	defer reader.Close()

	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	iter := reader.IterCF(engine_util.CfLock)
	defer iter.Close()

	// collect keys
	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lock, err := mvcc.ParseLock(value)
		if err != nil {
			return resp, err
		}
		if lock.Ts == req.StartVersion {
			key := item.KeyCopy(nil)
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return resp, nil
	}

	if req.CommitVersion == 0 {
		// batch roll back
		rbReq :=  &kvrpcpb.BatchRollbackRequest{
			Keys: keys,
			StartVersion: txn.StartTS,
			Context: req.Context,
		}
		rbResp, err := server.KvBatchRollback(context.TODO(), rbReq)
		resp.Error = rbResp.Error
		resp.RegionError = rbResp.RegionError
		return resp, err
	} else {
		// commit
		cmReq := &kvrpcpb.CommitRequest{
			Keys: keys,
			StartVersion: txn.StartTS,
			CommitVersion: req.CommitVersion,
			Context: req.Context,
		}
		cmResp, err := server.KvCommit(context.TODO(), cmReq)
		resp.Error = cmResp.Error
		resp.RegionError = cmResp.RegionError
		return resp, err
	}
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
