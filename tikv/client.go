package tikv

import (
	"context"
	"time"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"google.golang.org/grpc"

	"github.com/cosven/easy/grpcutil"
)

type FailPoint struct {
	Name    string
	Actions string
}

type DebugClient interface {
	ListRegionStates(ctx context.Context) ([]*debugpb.PeerCurrentState, error)

	InjectFailPoint(ctx context.Context) error
	RecoverFailPoint(ctx context.Context) error
	ListFailPoints(ctx context.Context) ([]*FailPoint, error)

	Close() error
}

type debugClient struct {
	Addr string

	Timeout time.Duration

	// TODO: maybe we can maintain a connection pool like tidb/store/tikv/client.go
	// conns []*grpc.ClientConn
}

func NewDebugClient(addr string, timeout time.Duration) DebugClient {
	return &debugClient{
		Addr:    addr,
		Timeout: timeout,
	}
}

// getConn get or create a grpc connection
// Currently, we always create a connection
func (c *debugClient) getConn(ctx context.Context) (*grpc.ClientConn, error) {
	return grpcutil.GetClientConn(ctx, c.Addr, nil)
}

func (c *debugClient) ListRegionStates(ctx context.Context) ([]*debugpb.PeerCurrentState, error) {

}

func (c *debugClient) InjectFailPoint(ctx context.Context) error {
}

func (c *debugClient) RecoverFailPoint(ctx context.Context) error {
}

func (c *debugClient) ListFailPoints(ctx context.Context) ([]*FailPoint, error) {
	conn, err := c.getConn(ctx)
	// TODO: how to apply timeout?
}

func (c *debugClient) Close() {

}
