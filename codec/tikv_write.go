// WriteType and Write are ported from tikv:
// https://github.com/tikv/tikv/blob/master/components/txn_types/src/write.rs

package codec

import (
	"github.com/pingcap/errors"
	tidbcodec "github.com/pingcap/tidb/util/codec"
)

const (
	ShortValueMaxLen uint8 = 255
	ShortValuePrefix byte  = byte('v')
)

type WriteType int

const (
	WriteTypePut WriteType = iota
	WriteTypeDelete
	WriteTypeLock
	WriteTypeRollback
)

const (
	FlagWriteTypePut      byte = byte('P')
	FlagWriteTypeDelete   byte = byte('D')
	FlagWriteTypeLock     byte = byte('L')
	FlagWriteTypeRollback byte = byte('R')
)

func WriteTypeToFlag(writeType WriteType) (b byte) {
	switch writeType {
	case WriteTypePut:
		b = FlagWriteTypePut
	case WriteTypeDelete:
		b = FlagWriteTypeDelete
	case WriteTypeLock:
		b = FlagWriteTypeLock
	case WriteTypeRollback:
		b = FlagWriteTypeRollback
	default:
		panic("unknown write type")
	}
	return
}

func FlagToWriteType(b byte) (writeType WriteType, err error) {
	switch b {
	case FlagWriteTypePut:
		writeType = WriteTypePut
	case FlagWriteTypeDelete:
		writeType = WriteTypeDelete
	case FlagWriteTypeLock:
		writeType = WriteTypeLock
	case FlagWriteTypeRollback:
		writeType = WriteTypeRollback
	default:
		err = errors.Errorf("unknown write type flag %v", b)
	}
	return
}

type Write struct {
	WriteType  WriteType
	StartTs    uint64
	ShortValue []byte

	HasOverlappedRollback bool
}

// NewWriteFromValue decodes value to Write
//
// value: <flag:byte><start_ts:uint64>v<len:byte><user_value:[]byte><...>
func NewWriteFromValue(value []byte) (*Write, error) {
	b, flag, err := DecodeUint8(value)
	if err != nil {
		return nil, err
	}
	writeType, err := FlagToWriteType(flag)
	if err != nil {
		return nil, err
	}
	b, startTs, err := tidbcodec.DecodeUvarint(b)
	if err != nil {
		return nil, err
	}
	// read the short value prefix b'v'
	// FIXME: maybe not a short value
	b, _, err = DecodeUint8(b)
	if err != nil {
		return nil, err
	}
	b, length, err := DecodeUint8(b)
	if err != nil {
		return nil, err
	}
	if len(b) < int(length) {
		err = errors.Errorf(
			"invalid value, content len %d shorter that value len %d", len(b), length)
		return nil, err
	}
	shortValue := b[:length]
	// TODO: parse the remain byte
	return &Write{
		WriteType:  writeType,
		StartTs:    startTs,
		ShortValue: shortValue,
	}, nil
}

// ToValue encodes a Write instance to bytes
func (w *Write) ToValue() (buf []byte) {
	flag := WriteTypeToFlag(w.WriteType)
	buf = append(buf, flag)
	buf = tidbcodec.EncodeUvarint(buf, w.StartTs)
	length := len(w.ShortValue)
	// please ensure ShortValue is valid, we check the length to prevent
	// unexpected condition
	if length > int(ShortValueMaxLen) {
		panic("invalid short value, too large")
	}
	buf = append(buf, ShortValuePrefix)
	buf = append(buf, byte(length))
	buf = append(buf, w.ShortValue...)
	return
}
