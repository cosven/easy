package cases

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

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
	return key
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
