package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf *config.Config
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		conf: conf,
		engine: engine_util.NewEngines(engine_util.CreateDB(conf.DBPath, false), nil, conf.DBPath, ""),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneStorageReader(s.engine.Kv)
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	wb := new(engine_util.WriteBatch)
	for _, m := range batch {
		wb.SetCF(m.Cf(), m.Key(), m.Value())
	}
	return s.engine.WriteKV(wb)
}

type StandAloneStorageReader struct {
	kv *badger.DB
	txn *badger.Txn
	iter *engine_util.BadgerIterator
}

func NewStandAloneStorageReader(db *badger.DB) (*StandAloneStorageReader, error) {
	return &StandAloneStorageReader{
		kv: db,
		txn: nil,
	}, nil
}

func (r *StandAloneStorageReader)	GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(r.kv, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneStorageReader)	IterCF(cf string) engine_util.DBIterator {
	r.txn = r.kv.NewTransaction(false)
	r.iter = engine_util.NewCFIterator(cf, r.txn)
	return r.iter
}

func (r *StandAloneStorageReader)	Close() {
	if r.iter != nil {
		r.iter.Close()
	}
	if r.txn != nil {
		r.txn.Discard()
	}
}
