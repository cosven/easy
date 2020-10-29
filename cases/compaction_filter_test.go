package cases

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/cosven/easy/codec"
	"github.com/cosven/easy/easytidb"
	"github.com/cosven/easy/easytikv/debug"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"go.uber.org/zap"
)

func failIfError(t *testing.T, err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func triggerGCASAP() error {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/test")
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`update mysql.tidb set VARIABLE_VALUE="1m" where VARIABLE_NAME="tikv_gc_run_interval";`)
	return err
}

func TestPrepare(t *testing.T) {
	failIfError(t, triggerGCASAP())
}

func TestWriteRow(t *testing.T) {
	pdAddrs := []string{"127.0.0.1:2379"}
	tidbStatusAddr := "127.0.0.1:10080"

	prepareTable := func() {
		db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:4000)/test")
		failIfError(t, err)
		defer db.Close()
		_, err = db.Exec("drop table if exists t;")
		failIfError(t, err)
		_, err = db.Exec("create table t(i int primary key, name varchar(255));")
		failIfError(t, err)
	}
	prepareTable()

	kvCli, err := rawkv.NewClient(context.TODO(), pdAddrs, config.Default())
	failIfError(t, err)
	defer kvCli.Close()
	tableInfo, err := easytidb.GetTableInfoFromTidbStatus(tidbStatusAddr, "test", "t")
	failIfError(t, err)
	rowId := int64(2334)
	key := codec.EncodeIntRowKey(tableInfo.ID, rowId)

	var oldRawKey, oldRawValue []byte

	prepareRows := func() {
		safepoint, err := GetSafePointFromPd(pdAddrs)
		failIfError(t, err)
		physical := safepoint >> 18
		log.Info("got safepoint", zap.Uint64("safepoint", safepoint))

		oldStartTs := (physical - 4) << 18
		oldCommitTs := (physical - 3) << 18
		newStartTs := (physical - 2) << 18
		newCommitTs := (physical - 1) << 18

		oldRawKey = codec.EncodeWriteKey(key, oldCommitTs)
		newRawKey := codec.EncodeWriteKey(key, newCommitTs)

		// construct value
		oldRow := []types.Datum{types.NewIntDatum(2333), types.NewStringDatum("Alex")}
		newRow := []types.Datum{types.NewIntDatum(2333), types.NewStringDatum("Alex!!")}
		colIDs := make([]int64, len(tableInfo.Columns))
		for i, col := range tableInfo.Columns {
			colIDs[i] = col.ID
		}
		oldValue, err := SimpleEncodeRow(oldRow, colIDs)
		newValue, err := SimpleEncodeRow(newRow, colIDs)
		failIfError(t, err)

		// construct tikv value
		oldWrite := codec.Write{
			WriteType:  codec.WriteTypePut,
			StartTs:    oldStartTs,
			ShortValue: oldValue,
		}
		newWrite := codec.Write{
			WriteType:  codec.WriteTypePut,
			StartTs:    newStartTs,
			ShortValue: newValue,
		}
		oldRawValue = oldWrite.ToValue()
		newRawValue := newWrite.ToValue()
		err = kvCli.Put(context.TODO(), oldRawKey, oldRawValue,
			rawkv.PutOption{Cf: rawkv.CfWrite})
		failIfError(t, err)
		err = kvCli.Put(context.TODO(), newRawKey, newRawValue,
			rawkv.PutOption{Cf: rawkv.CfWrite})
		failIfError(t, err)
	}
	prepareRows()

	// manual compact
	debugCli := debug.NewDebugClient("http://127.0.0.1:20160")
	err = debugCli.Compact(context.TODO(), debugpb.DB_KV, "write",
		debug.WithBottommostLevelCompaction(debugpb.BottommostLevelCompaction_Force))
	failIfError(t, err)

	// wait for compaction finish
	// FIXME
	time.Sleep(time.Second * 5)

	value, err := kvCli.Get(context.TODO(), oldRawKey, rawkv.GetOption{Cf: rawkv.CfWrite})
	failIfError(t, err)
	if bytes.Equal(value, oldRawValue) {
		println("failed")
	} else {
		println("ok")
	}

	keys, _, err := kvCli.Scan(context.TODO(), key, nil, 100, rawkv.ScanOption{Cf: rawkv.CfWrite})
	failIfError(t, err)
	println(len(keys))
	for _, key := range keys {
		println(string(key))
	}

}

func TestScan(t *testing.T) {
	pdAddrs := []string{"127.0.0.1:2379"}
	tidbStatusAddr := "127.0.0.1:10080"

	kvCli, err := rawkv.NewClient(context.TODO(), pdAddrs, config.Default())
	failIfError(t, err)
	defer kvCli.Close()
	tableInfo, err := easytidb.GetTableInfoFromTidbStatus(tidbStatusAddr, "test", "t")
	failIfError(t, err)

	// construct key
	rowId := int64(1)

	key := codec.EncodeIntRowKey(tableInfo.ID, rowId)

	keys, _, err := kvCli.Scan(context.TODO(), key, nil, 100, rawkv.ScanOption{Cf: rawkv.CfWrite})
	failIfError(t, err)
	println(len(keys))
	for _, key := range keys {
		_, rowId, err = codec.DecodeIntRowKey(key)
		failIfError(t, err)
		println(rowId)
	}
}
