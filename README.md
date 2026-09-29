# 嵌入式键值引擎

这是一个直接嵌入 Go 业务进程的本地文件键值引擎原型，不依赖外部数据库服务。

## 使用方式

```go
db, err := kvengine.Open("./data")
if err != nil {
    return err
}
defer db.Close()

if err := db.WriteBatch([]kvengine.Op{
    kvengine.Put("user:1", []byte("Alice")),
    kvengine.Delete("user:old"),
}); err != nil {
    return err
}

snap, err := db.Snapshot()
if err != nil {
    return err
}
defer snap.Close()

value, ok, err := snap.Get("user:1")
```

## 首轮保证

- `WriteBatch` 以“数据帧 + 提交帧”写入 WAL，并在提交帧 `fsync` 成功后才更新内存状态。
- 每帧包含帧头校验和与 payload 校验和；崩溃造成的不完整尾部会回退到上一个提交边界。
- 完整帧的校验和、类型、批内容或提交信息错误会返回 `ErrCorrupted`，不会被当作日志正常结束。
- 快照保留创建时的序列号，后续覆盖和删除不会改变旧快照；快照关闭后可回收不再需要的历史版本。
- 恢复时重新按提交顺序生成序列号，未提交数据帧不会部分生效。

## 验证

运行：

```bash
go test ./...
go test -race ./...
```

测试会构造两批已提交历史，再对后续两批日志做从 0 到完整长度的逐字节截断；每一种中断结果都与独立维护的提交后键值模型逐项比对。测试还覆盖完整记录损坏、提交帧撕裂、批原子性和旧快照隔离。
