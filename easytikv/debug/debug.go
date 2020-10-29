package debug

import (
	"context"

	"github.com/cosven/easy/grpcutil"

	"github.com/pingcap/kvproto/pkg/debugpb"
	"google.golang.org/grpc"
)

type FailPoint struct {
	Name    string
	Actions string
}

// DebugReqSender is designed to establish non-persistent grpc connection
// and send debug request.
type debugReqSender struct {
	cli  debugpb.DebugClient
	conn *grpc.ClientConn
}

func newDebugReqSender(ctx context.Context, addr string) (*debugReqSender, error) {
	conn, err := grpcutil.GetClientConn(ctx, addr, nil)
	if err != nil {
		return nil, err
	}
	cli := debugpb.NewDebugClient(conn)
	return &debugReqSender{
		cli:  cli,
		conn: conn,
	}, nil
}

type DebugClient struct {
	Addr string
}

func (s *debugReqSender) Close() {
	s.conn.Close()
}

func NewDebugClient(addr string) *DebugClient {
	return &DebugClient{
		Addr: addr,
	}
}

func (c *DebugClient) Compact(ctx context.Context, db debugpb.DB, cf string, opts ...CompactOption) error {
	op := NewCompactOp(db, cf, opts)
	req := op.toReq()
	sender, err := newDebugReqSender(ctx, c.Addr)
	if err != nil {
		return err
	}
	defer sender.Close()
	_, err = sender.cli.Compact(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (c *DebugClient) InjectFailPoint(ctx context.Context) error {
	return nil
}

func (c *DebugClient) RecoverFailPoint(ctx context.Context) error {
	return nil
}

func (c *DebugClient) ListFailPoints(ctx context.Context) ([]*FailPoint, error) {
	return nil, nil
}
