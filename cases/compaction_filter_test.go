package cases

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/cosven/easy/codec"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

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

func TestGetSafePoint(t *testing.T) {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/test")
	if err != nil {
		panic(err)
	}
	rows, err := db.Query(`select VARIABLE_VALUE from mysql.tidb where VARIABLE_NAME = "tikv_gc_safe_point";`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var safePoint time.Time
	rows.Next()
	rows.Scan(safePoint)
	fmt.Println(safePoint)
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

	key := codec.EncodeIntRowKey(45, 2)
	commitTs := uint64(420326424217649154)
	kvKey := codec.EncodeWriteKey(key, commitTs)
	value, err := cli.Get(context.TODO(), kvKey, rawkv.GetOption{Cf: rawkv.CfWrite})
	if err != nil {
		panic(err)
	}
	write, err := codec.NewWriteFromValue(value)
	if err != nil {
		panic(err)
	}
	DecodeRow(write.ShortValue)
}
