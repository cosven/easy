package cases

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

// x use modules
func x() string {
	if bytes.Equal([]byte{'x'}, []byte{'y'}) {
		return hex.EncodeToString([]byte{'z'})
	}
	return "miao"
}

func decodeKey(key []byte) error {
	b, decoded, err := codec.DecodeBytes(key, nil)
	if err != nil {
		return err
	}
	if len(decoded) == 19 && decoded[0] == 't' && decoded[9] == '_' && decoded[10] == 'r' {
		_, table_id, _ := codec.DecodeInt(decoded[1:9])
		_, row_id, _ := codec.DecodeInt(decoded[11:19])

		fmt.Printf("table_id: %v, row_id: %v\n", table_id, row_id)

		if len(b) == 8 {
			_, ts, _ := codec.DecodeUintDesc(b)
			ms := int64(ts >> 18)
			fmt.Printf("ts: %v (%v)\n", ts, time.Unix(ms/1e3, (ms%1e3)*1e6))
		}
	}
	return nil
}

func encodeRowKey(tableId, rowId int64) []byte {
	key := []byte{'t'}
	key = codec.EncodeInt(key, tableId)
	key = append(key, []byte("_r")...)
	key = codec.EncodeInt(key, rowId)
	return codec.EncodeBytes([]byte{}, key)
}

func KvEncodeKey(key []byte, ts uint64) []byte {
	return codec.EncodeUintDesc(key, ts)
}

func EncodeValue(writeType []byte, startTs uint64, value []byte) {

}

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

func DecodeValue(b []byte) error {
	b, writeType, err := DecodeUint8(b)
	if err != nil {
		return err
	}
	fmt.Printf("write_type: %v\n", string(byte(writeType)))
	b, startTs, err := codec.DecodeUvarint(b)
	if err != nil {
		return err
	}
	fmt.Printf("start_ts: %v\n", startTs)
	// TODO: currently, we assume the value is a shortvalue
	// read the short value prefix b'v'
	b, _, err = DecodeUint8(b)
	if err != nil {
		return err
	}
	b, length, err := DecodeUint8(b)
	if err != nil {
		return err
	}
	if len(b) < int(length) {
		panic(fmt.Sprintf("content len %d shorter that value len %d", len(b), length))
	}
	value := b[:length]
	// TODO: currently, we ignore the remain bytes
	fmt.Printf("value: %s\n", string(value))
	DecodeRow(value)
	return nil
}

func DecodeRow(value []byte) error {
	resp, err := http.Get("http://127.0.0.1:10080/schema/test/t")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	var tableInfo model.TableInfo
	err = json.Unmarshal(body, &tableInfo)
	if err != nil {
		return err
	}
	colMap := make(map[int64]*types.FieldType, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		colMap[col.ID] = &col.FieldType
	}

	rs, err := tablecodec.DecodeRowToDatumMap(value, colMap, time.UTC)
	for _, col := range tableInfo.Columns {
		if c, ok := rs[col.ID]; ok {
			data := "nil"
			if !c.IsNull() {
				data, err = c.ToString()
			}
			fmt.Printf("name: %s, value: %s\n", col.Name.O, data)
		}
	}
	return nil
}

func TestDecodeRow(t *testing.T) {
	data, err := base64.StdEncoding.DecodeString("gAABAAAAAgQATWFyeQ==")
	if err != nil {
		panic(err)
	}
	if err := DecodeRow(data); err != nil {
		panic(err)
	}
}

func TestRawkv(t *testing.T) {
	cli, err := rawkv.NewClient(context.TODO(), []string{"127.0.0.1:2379"}, config.Default())
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	key := encodeRowKey(45, 2)
	commitTs := uint64(420326424217649154)
	kvKey := KvEncodeKey(key, commitTs)
	value, err := cli.Get(context.TODO(), kvKey, rawkv.GetOption{Cf: rawkv.CfWrite})
	if err != nil {
		panic(err)
	}
	err = DecodeValue(value)
	if err != nil {
		panic(err)
	}
}
