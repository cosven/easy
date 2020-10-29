package debug

import (
	"github.com/pingcap/kvproto/pkg/debugpb"
)

type CompactOp struct {
	db                        debugpb.DB
	cf                        string
	fromKey                   []byte
	toKey                     []byte
	threads                   uint32
	bottommostLevelCompaction debugpb.BottommostLevelCompaction
}

type CompactOption func(*CompactOp)

func (op *CompactOp) applyOpt(opts []CompactOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// NewCompactOp creates a CompactOp instance
func NewCompactOp(db debugpb.DB, cf string, opts []CompactOption) CompactOp {
	ret := CompactOp{db: db, cf: cf}
	ret.applyOpt(opts)
	return ret
}

func (op CompactOp) toReq() *debugpb.CompactRequest {
	return &debugpb.CompactRequest{
		Db:                        op.db,
		Cf:                        op.cf,
		FromKey:                   op.fromKey,
		ToKey:                     op.toKey,
		Threads:                   op.threads,
		BottommostLevelCompaction: op.bottommostLevelCompaction,
	}
}

func WithRange(fromKey, toKey []byte) CompactOption {
	return func(op *CompactOp) {
		op.fromKey = fromKey
		op.toKey = toKey
	}
}

func WithThreads(threads uint32) CompactOption {
	return func(op *CompactOp) { op.threads = threads }
}

func WithBottommostLevelCompaction(c debugpb.BottommostLevelCompaction) CompactOption {
	return func(op *CompactOp) { op.bottommostLevelCompaction = c }
}
