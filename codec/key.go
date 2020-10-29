package codec

import (
	"github.com/pingcap/errors"
	tidbcodec "github.com/pingcap/tidb/util/codec"
)

// EncodeTableRowKey encodes table_id+row_id for a row
//
// rowId is also known as handleId in TiDB
// NOTE: This function is similar to tablecodec.EncodeRowKey, but it
// depends less tidb data structure and package
func EncodeIntRowKey(tableId, rowId int64) []byte {
	key := []byte{'t'}
	key = tidbcodec.EncodeInt(key, tableId)
	key = append(key, []byte("_r")...)
	key = tidbcodec.EncodeInt(key, rowId)
	return tidbcodec.EncodeBytes([]byte{}, key)
}

func DecodeIntRowKey(key []byte) (tableId, rowId int64, err error) {
	_, decoded, err := tidbcodec.DecodeBytes(key, nil)
	if err != nil {
		return 0, 0, nil
	}
	if len(decoded) == 19 && decoded[0] == 't' && decoded[9] == '_' && decoded[10] == 'r' {
		_, table_id, _ := tidbcodec.DecodeInt(decoded[1:9])
		_, row_id, _ := tidbcodec.DecodeInt(decoded[11:19])
		return table_id, row_id, nil
	} else {
		return 0, 0, errors.New("invalid row key")
	}
}

// EncodeWriteKey encodes user_key+commit_ts to a key in write cf
//
// <user_key:[]byte><commit_ts:uint64>
func EncodeWriteKey(key []byte, commitTs uint64) []byte {
	return tidbcodec.EncodeUintDesc(key, commitTs)
}

// DecodeWriteKey decodes the EncodeWriteKey value
func DecodeWriteKey(key []byte) ([]byte, uint64, error) {
	tsLength := 8
	if len(key) < tsLength {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}
	pos := len(key) - tsLength
	tsBytes := key[pos:]
	key = key[:pos]
	_, ts, err := tidbcodec.DecodeUintDesc(tsBytes)
	if err != nil {
		return nil, 0, err
	}
	return key, ts, nil
}
