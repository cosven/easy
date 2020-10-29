package cases

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cosven/easy/codec"
	"github.com/cosven/easy/easytidb"
	"github.com/cosven/easy/easytikv/debug"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/rawkv"
)

func failIfError(t *testing.T, err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

const (
	dbName    string = "test"
	tableName string = "t"
	rowId     int64  = 2333
)

// TableTestT represents the table `t` in database `test`
type TableTestT struct {
	id     int64
	colIDs []int64
}

func mustNewTestT(tc tidbCluster) *TableTestT {
	tableInfo, err := easytidb.GetTableInfoFromTidbStatus(tc.tidbStatusAddr, "test", "t")
	if err != nil {
		log.Fatal(err.Error())
	}
	colIDs := make([]int64, len(tableInfo.Columns))
	for i, col := range tableInfo.Columns {
		colIDs[i] = col.ID
	}
	return &TableTestT{
		id:     tableInfo.ID,
		colIDs: colIDs,
	}
}

func (t *TableTestT) mustGenRowKV(rowId int64, startTs, commitTs uint64) ([]byte, []byte) {
	key := codec.EncodeIntRowKey(t.id, rowId)
	rawKey := codec.EncodeWriteKey(key, commitTs)

	name := fmt.Sprintf("Alex - %d", commitTs)
	row := []types.Datum{types.NewIntDatum(rowId), types.NewStringDatum(name)}

	value, err := SimpleEncodeRow(row, t.colIDs)
	if err != nil {
		log.Fatal(err.Error())
	}
	write := codec.Write{
		WriteType:  codec.WriteTypePut,
		StartTs:    startTs,
		ShortValue: value,
	}
	rawValue := write.ToValue()
	return rawKey, rawValue
}

func TestSetup(t *testing.T) {
	tc := NewSimpleTidbCluster()
	tc.triggerGCASAP()
	tc.mustExecSql("drop table if exists t;")
	tc.mustExecSql("create table t(id int primary key, name varchar(255));")
}

func TestCompactShouldDeleteOldKey(t *testing.T) {
	tc := NewSimpleTidbCluster()
	kvCli := tc.mustNewRawkvClient()
	defer kvCli.Close()
	debugCli := tc.mustNewDebugClient()
	tableT := mustNewTestT(*tc)

	// insert two keys(commit_ts < safepoint) into write cf
	// the old key is supposed to be delete in next gc round
	safepoint := tc.mustGetSafePoint()
	physical := safepoint >> 18
	oldStartTs := (physical - 2) << 18
	oldCommitTs := oldStartTs + 1000
	newStartTs := (physical - 1) << 18
	newCommitTs := newStartTs + 1000
	rowId := int64(2333)

	oldRawKey, oldRawValue := tableT.mustGenRowKV(rowId, oldStartTs, oldCommitTs)
	newRawKey, newRawValue := tableT.mustGenRowKV(rowId, newStartTs, newCommitTs)

	err := kvCli.Put(context.TODO(), oldRawKey, oldRawValue, rawkv.PutOption{Cf: rawkv.CfWrite})
	failIfError(t, err)
	err = kvCli.Put(context.TODO(), newRawKey, newRawValue, rawkv.PutOption{Cf: rawkv.CfWrite})
	failIfError(t, err)

	// trigger compaction on write cf
	err = debugCli.Compact(context.TODO(), debugpb.DB_KV, "write",
		debug.WithBottommostLevelCompaction(debugpb.BottommostLevelCompaction_Force))
	failIfError(t, err)

	// wait for compaction finish
	// FIXME
	log.Info("sleep 5 seconds to wait for compaction finish")
	time.Sleep(time.Second * 5)

	// the old value should be gc during compaction
	value, err := kvCli.Get(context.TODO(), oldRawKey, rawkv.GetOption{Cf: rawkv.CfWrite})
	failIfError(t, err)
	if bytes.Equal(value, oldRawValue) {
		t.FailNow()
	}
}

// TestScan is used for debug
func TestDebug(t *testing.T) {
	tc := NewSimpleTidbCluster()
	safepoint := tc.mustGetSafePoint()
	kvCli := tc.mustNewRawkvClient()
	defer kvCli.Close()
	tableT := mustNewTestT(*tc)
	key := codec.EncodeIntRowKey(tableT.id, 0)
	keys, _, err := kvCli.Scan(context.TODO(), key, nil, 100, rawkv.ScanOption{Cf: rawkv.CfWrite})
	failIfError(t, err)
	fmt.Printf("total keys: %d\n", len(keys))
	fmt.Printf("safepoint: %d\n", safepoint)
	for _, key := range keys {
		userKey, ts, err := codec.DecodeWriteKey(key)
		if err != nil {
			continue
		}
		_, rowId, err := codec.DecodeIntRowKey(userKey)
		failIfError(t, err)
		fmt.Printf("%d, %d, %v\n", rowId, ts, ts < safepoint)
	}
}
