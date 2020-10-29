module github.com/cosven/easy

go 1.14

replace github.com/tikv/client-go v0.0.0-20201015080021-528475568618 => github.com/cosven/client-go v0.0.0-20201029085241-63a1539d6469

replace go.etcd.io/etcd/v3 => github.com/etcd-io/etcd/v3 v3.3.0-rc.0.0.20200826232710-c20cc05fc548

replace github.com/pingcap/kvproto v0.0.0-20201023092649-e6d6090277c9 => github.com/gengliqi/kvproto v0.0.0-20200902152943-af12ea861cbf

require (
	github.com/go-sql-driver/mysql v1.5.0
	github.com/pingcap/errors v0.11.5-0.20201021055732-210aacd3fd99
	github.com/pingcap/kvproto v0.0.0-20201027123903-c4791e779a8c
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463
	github.com/pingcap/parser v0.0.0-20201024025010-3b2fb4b41d73
	github.com/pingcap/tidb v1.1.0-beta.0.20201026110301-928c35de796e
	github.com/tikv/client-go v0.0.0-20201015080021-528475568618
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.16.0
	google.golang.org/grpc v1.26.0
)
