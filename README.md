# TiDB System Testing Framework for Humans

## Compaction Filter 用例运行简单说明

### 启动 tidb 集群

运行用例前，需要使用启动 tidb 集群，1tidb + 1tikv + 1pd，推荐使用 tiup playground 启动。
```sh
tiup playground nightly --tiflash 0 --kv.config tikv.toml
```

一个参考的 tikv 配置
```toml
log-level = "debug"

[gc]
enable-compaction-filter = true
compaction-filter-skip-version-check = true

[raftstore]
consistency-check-interval = "180s"  # seconds

[rocksdb]
[rocksdb.writecf]
block-size = "32KB"
write-buffer-size = "4MB"
target-file-size-base = "4MB"
```

### 运行用例

1. 运行 `go test -v -run TestSetup -count 1 ./cases/`
   这个会调整 gc run interval，并在 test 下创建表 t。
   并等待一两分钟，目的是等待 gc 触发，safepoint 非 0。

2. 运行 `go test -v -run TestCompactShouldDeleteOldKey -count 1 ./cases/`，
   这是一个测试用例。它会使用 rawkv，往 write cf 插入两条数据。
   这两条数据的 `commit_ts` 都小于 safepoint，我们期望两条数据中较老的那一条会被 (compaction) gc 删除。

   ps: 这两条数据是模拟 tidb 编码规则的，对应的同一行，通过 `select * from t` 理论上是可以扫到的。

3. 调试（目前加了个用例用来方便调试） `go test -v -run TestDebug -count 1 ./cases/`
