# kvengine

`kvengine` 是一个可直接嵌入 Go 进程的本地键值引擎，不依赖或安装外部数据库。当前首轮实现聚焦三件事：

- 批量写入的原子性与持久化
- 不受后续写入影响的读取快照
- 进程在任意写入边界崩溃后的恢复

数据保存在数据目录的 `kv.wal`。每条 WAL 记录包含 magic、版本、类型、payload 长度和 CRC32C 校验和。

## 用法

```go
db, info, err := kvengine.Open("./mydata")
if err != nil {
    return err
}
defer db.Close()

err = db.Batch([]kvengine.Op{
    {Kind: kvengine.Set, Key: "user:1", Value: []byte("Ada")},
    {Kind: kvengine.Set, Key: "counter", Value: []byte("1")},
})

value, ok, err := db.Get("user:1")
snapshot, err := db.Snapshot()
```

`Snapshot()` 返回深拷贝。快照创建后，后续覆盖、删除或新批次都不会改变它；调用方修改返回的 map 或 value 也不会影响引擎。

## 原子提交

一个 `Batch` 先编码为单条 WAL 记录，然后：

1. 从当前 WAL 末尾追加完整帧；
2. `fsync` 成功；
3. 更新内存状态。

因此一批修改在成功返回后全部可见；若写入或同步失败、或进程在提交帧完整落盘前中断，则整批不可见。

## 恢复语义

打开数据库时从头重放完整且校验通过的记录：

- 头部或 payload 不完整：视为崩溃造成的截断，删除从最后一条完整记录之后开始的尾部，并返回 `RecoveryInfo.Truncated=true`；
- 完整记录 magic/版本/类型错误、CRC 不匹配或 payload 语义非法：返回 `ErrCorruptWAL`，不会把坏记录当正常结束，也不会静默截断；
- 干净关闭后重开：不截断，`RecoveryInfo.Truncated=false`。

截断修复会再次 `fsync`，后续打开不会重复处理同一残缺尾部。

## 验证

测试使用一个独立于引擎实现的 `map` 参考模型重放读写历史，并逐批对照引擎快照。崩溃样例会构造两条已提交记录和一条未提交批次，然后对未提交帧从第 1 字节到倒数第 1 字节逐字节截断，验证每个写入边界：

- 已提交历史完整保留；
- 未提交批次中的任何操作都不可见；
- WAL 精确截回最后完整记录；
- 二次打开无需再恢复；
- 完整但被篡改的记录返回 `ErrCorruptWAL`。

运行：

```sh
go test ./...
go test -race ./...
go vet ./...
```
