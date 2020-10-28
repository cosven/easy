package debug

import (
	"github.com/pingcap/kvproto/pkg/debugpb"
)

type CompactOp struct {
	Db                        debugpb.DB
	Cf                        string
	FromKey                   []byte
	ToKey                     []byte
	Threads                   uint32
	BottommostLevelCompaction debug.BottommostLevelCompaction
}

type CompactOption func(*CompactOp)

func (op *CompactOp) applyOpt(opts []CompactOption) {

}

func OpCompact(Db debugpb.DB, opts ...CompactOp) {
	ret := CompactOp{Db: Db}

}

func (op CompactOp) toReq() *debugpb.CompactRequest {

}
