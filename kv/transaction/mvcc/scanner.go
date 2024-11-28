package mvcc

import (
	"bytes"

	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	NextKey	[]byte
	Txn			*MvccTxn
	Iter		engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(startKey, txn.StartTS))
	return &Scanner{
		NextKey: startKey,
		Txn: txn,
		Iter: iter,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.Iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if scan.NextKey == nil || !scan.Iter.Valid() {
		return nil, nil, nil
	}

	currentKey := scan.NextKey
	currentValue, err := scan.Txn.GetValue(scan.NextKey)
	if err != nil {
		return nil, nil, err
	}

	// find the next key
	scan.NextKey = nil
	for ; scan.Iter.Valid(); scan.Iter.Next() {
		wItem := scan.Iter.Item()
		wKey := wItem.KeyCopy(nil)
		userKey := DecodeUserKey(wKey)
		if !bytes.Equal(currentKey, userKey) {
			scan.NextKey = userKey
			break
		}
	}

	return currentKey, currentValue, nil
}
