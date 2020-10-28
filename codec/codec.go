package codec

import (
	"bytes"
	"encoding/binary"

	"github.com/pingcap/errors"
)

// DecodeUint8 decodes value encoded by tikv(to_u8) before
//
// TiDB codec package implement DecodeUint64 and DecodeXxx, but there is
// no DecodeUint8. Implement one here.
func DecodeUint8(b []byte) ([]byte, uint8, error) {
	if len(b) < 1 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	var v uint8
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.BigEndian, &v)
	if err != nil {
		return b, 0, err
	}
	b = b[1:]
	return b, v, nil
}
