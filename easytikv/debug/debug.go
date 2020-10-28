package tikv

import (
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rpc"
	"google.golang.org/grpc"
)

type FailPoint struct {
	Name    string
	Actions string
}

type DebugClient struct {
	Addr      string
	rpcClient rpc.Client
}

func DefaultCompactOption() {

}

func NewDebugClient(addr string, config config.RPC) DebugClient {
	return &debugClient{
		Addr:      addr,
		rpcClient: rpc.NewRPCClient(&conf.RPC),
	}
}

func (c *debugClient) Close() error {
	return c.rpcClient.Close()
}

func (c *debugClient) Compact(ctx context.Context, db debugpb.DB, cf string,
	fromKey []byte, toKey []byte, threads uint32, bottommostLevelCompaction debugpb.BottommostLevelCompaction) error {

}

func (c *debugClient) InjectFailPoint(ctx context.Context) error {
	return nil
}

func (c *debugClient) RecoverFailPoint(ctx context.Context) error {
	return nil
}

func (c *debugClient) ListFailPoints(ctx context.Context) ([]*FailPoint, error) {
	return nil, nil
}
