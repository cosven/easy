// tmp.go 存放了一些暂时还没想好放哪个 package 的工具函数

package cases

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.etcd.io/etcd/clientv3"
)

func GetSafePointFromPd(pdAddrs []string) (uint64, error) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: pdAddrs})
	if err != nil {
		return 0, err
	}
	gcSafePointKey := "/tidb/store/gcworker/saved_safe_point"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	resp, err := cli.Get(ctx, gcSafePointKey)
	cancel()
	if err != nil {
		return 0, err
	}
	if len(resp.Kvs) > 0 {
		value := string(resp.Kvs[0].Value)
		t, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, err
		}
		return t, nil
	} else {
		return 0, errors.New("no gc safe point key in pd/etcd")
	}

}

func SimpleEncodeRow(row []types.Datum, colIDs []int64) ([]byte, error) {
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	rd := rowcodec.Encoder{Enable: true}
	value, err := tablecodec.EncodeRow(sc, row, colIDs, nil, nil, &rd)
	if err != nil {
		return nil, err
	}
	return value, nil
}
