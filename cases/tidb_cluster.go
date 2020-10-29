package cases

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cosven/easy/easytikv/debug"

	"github.com/pingcap/log"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

// tidbCluster represents a tidb tidbCluster which has 1pd/1tikv/1tidb
type tidbCluster struct {
	pdAddrs        []string
	tidbStatusAddr string
	tidbAddr       string
	tikvAddr       string
}

func (tc *tidbCluster) mustExecSql(q string) sql.Result {
	dsn := fmt.Sprintf("root:@tcp(%s)/test", tc.tidbAddr)
	// TODO: reuse db instance
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()
	ret, err := db.Exec(q)
	if err != nil {
		log.Fatal(err.Error())
	}
	return ret
}

func (tc *tidbCluster) mustNewRawkvClient() *rawkv.Client {
	cli, err := rawkv.NewClient(context.TODO(), tc.pdAddrs, config.Default())
	if err != nil {
		log.Fatal(err.Error())
	}
	return cli
}

func (tc *tidbCluster) mustNewDebugClient() *debug.DebugClient {
	return debug.NewDebugClient(fmt.Sprintf("http://%s", tc.tikvAddr))
}

func (tc *tidbCluster) mustGetSafePoint() uint64 {
	safepoint, err := GetSafePointFromPd(tc.pdAddrs)
	if err != nil {
		log.Fatal(err.Error())
	}
	return safepoint
}

// triggerGCASAP update the gc interval to trigger GC as soon as possible
//
// A tidb tidbCluster has no safepoint before first GC.
func (tc *tidbCluster) triggerGCASAP() {
	tc.mustExecSql(`update mysql.tidb set VARIABLE_VALUE="1m" where VARIABLE_NAME="tikv_gc_run_interval";`)
}

func NewSimpleTidbCluster() *tidbCluster {
	pdAddrs := []string{"127.0.0.1:2379"}
	tidbStatusAddr := "127.0.0.1:10080"
	tidbAddr := "127.0.0.1:4000"
	tikvAddr := "127.0.0.1:20160"

	return &tidbCluster{
		pdAddrs:        pdAddrs,
		tidbAddr:       tidbAddr,
		tidbStatusAddr: tidbStatusAddr,
		tikvAddr:       tikvAddr,
	}
}
