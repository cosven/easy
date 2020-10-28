package cases

import (
	"context"
	"testing"

	"github.com/cosven/easy/codec"
	easytidb "github.com/cosven/easy/easytidb"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

func failIfError(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}

func TestWriteRow(t *testing.T) {
	pdAddrs := []string{"127.0.0.1:2379"}
	tidbStatusAddr := "127.0.0.1:10080"

	kvCli, err := rawkv.NewClient(context.TODO(), pdAddrs, config.Default())
	failIfError(t, err)
	defer kvCli.Close()
	tableInfo, err := easytidb.GetTableInfoFromTidbStatus(tidbStatusAddr, "test", "t")
	failIfError(t, err)
	safepoint, err := GetSafePointFromPd(pdAddrs)
	failIfError(t, err)
	physical := safepoint >> 18

	// construct key
	rowId := int64(2333)
	oldStartTs := (physical - 4) << 18
	oldCommitTs := (physical - 3) << 18
	newStartTs := (physical - 2) << 18
	newCommitTs := (physical - 1) << 18

	key := codec.EncodeIntRowKey(tableInfo.ID, rowId)
	oldRawKey := codec.EncodeWriteKey(key, oldCommitTs)
	newRawKey := codec.EncodeWriteKey(key, newCommitTs)

	// construct value
	oldRow := []types.Datum{types.NewIntDatum(2333), types.NewStringDatum("Alex")}
	newRow := []types.Datum{types.NewIntDatum(2333), types.NewStringDatum("Alex!")}
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
	oldRawValue := oldWrite.ToValue()
	newRawValue := newWrite.ToValue()
	err = kvCli.Put(context.TODO(), oldRawKey, oldRawValue,
		rawkv.PutOption{Cf: rawkv.CfWrite})
	failIfError(t, err)
	err = kvCli.Put(context.TODO(), newRawKey, newRawValue,
		rawkv.PutOption{Cf: rawkv.CfWrite})
	failIfError(t, err)
}
