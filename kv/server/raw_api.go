package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
  resp := &kvrpcpb.RawGetResponse{}
	reader, _ := server.storage.Reader(req.Context)
  defer reader.Close()
	value, _ := reader.GetCF(req.Cf, req.Key)
  resp.Value = value
  resp.NotFound = value == nil
  return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
  resp := &kvrpcpb.RawPutResponse{}
	err := server.storage.Write(req.Context, []storage.Modify {
		{
			Data: storage.Put {
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	})
  if err != nil {
    resp.Error = err.Error()
  }
  return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
  resp := &kvrpcpb.RawDeleteResponse{}
	err := server.storage.Write(req.Context, []storage.Modify {
		{
			Data: storage.Delete {
				Cf:    req.Cf,
				Key:   req.Key,
			},
		},
	})
  if err != nil {
    resp.Error = err.Error()
  }
  return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
  resp := &kvrpcpb.RawScanResponse{}
  reader, err := server.storage.Reader(req.Context)
  if err != nil {
    resp.Error = err.Error()
    return resp, nil
  }
  defer reader.Close()
  iter := reader.IterCF(req.Cf)
  kvs := []*kvrpcpb.KvPair{}
  cnt := 0
  for iter.Seek(req.StartKey); iter.Valid() && cnt < int(req.Limit); iter.Next() {
    value, err := iter.Item().ValueCopy(nil)
    if err != nil {
      kvs = append(kvs, &kvrpcpb.KvPair{
        Error: &kvrpcpb.KeyError{
          Abort: err.Error(),
        },
      })
    } else {
      kvs = append(kvs, &kvrpcpb.KvPair{
        Key: iter.Item().KeyCopy(nil),
        Value: value,
      })
    }
    cnt++
  }
  iter.Close()
  resp.Kvs = kvs
  return resp, nil
}
