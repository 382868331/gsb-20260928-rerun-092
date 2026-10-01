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

## 持久快照

内存快照只活在当前进程里；`SaveSnapshot(name)` 把当前已提交视图持久化到数据目录的 `snapshots/<name>.snap`，之后任何进程（不必是保存它的那个）都能使用：

```go
err = db.SaveSnapshot("before-migration")
names, err := db.ListSnapshots()          // 已保存快照名，按序返回
view, err := db.OpenSnapshot("before-migration") // 保存时刻的完整视图
err = db.ReleaseSnapshot("before-migration")
```

- 快照文件是带 CRC32C 校验的单条记录。保存先写临时文件并 `fsync`，再原子地以最终文件名生效并 `fsync` 目录；发布中断只残留临时文件，不会被列出或打开，下次 `Open` 时清除，因此不会暴露半份视图。
- 同名快照已存在时保存返回 `ErrSnapshotExists`，不会静默覆盖旧视图；先 `ReleaseSnapshot` 再保存即可替换。
- 保存后对键的覆盖与删除仍属于原快照；`OpenSnapshot` 返回深拷贝，调用方修改不影响已保存内容。
- `ReleaseSnapshot` 只删除该快照自身的文件；删除失败会返回错误且快照保持可用，不会误删有效数据。快照文件损坏（校验失败或截断）时 `OpenSnapshot` 返回 `ErrCorruptSnapshot`。
- 持久快照与日志整理并存：每个快照文件独立钉住自己引用的历史，`Compact()` 只回收 WAL 中不再被任何快照引用的旧版本，绝不触碰快照文件。重开后按任意顺序读当前视图与各旧快照，结果都与各自保存时一致。

## 范围查询

`Scan(start, end)` 返回 `[start, end)` 区间内按 key 排序的条目（`end` 为空表示无上限）。结果在调用开始时定格为一致视图：遍历返回切片期间发生的批量覆盖或删除不会混入，因此不会出现前半段旧值、后半段新值；修改返回的 value 也不会影响引擎。

## 日志整理

`Compact()` 把当前已提交的存活数据重写为单条批次记录，回收被覆盖和已删除条目占用的空间。整理过程：

1. 完整的新日志先写入 `kv.wal.tmp` 并 `fsync`；
2. 原子 `rename` 覆盖 `kv.wal`，再 `fsync` 目录；
3. 切换内存中的文件句柄。

`kv.wal` 本身从不被部分重写，因此任意阶段崩溃重开后，要么看到完整的旧日志、要么看到完整的新日志，不会暴露半批提交。残留的 `kv.wal.tmp` 在下一次 `Open` 时被丢弃。整理前获取的快照和 `Scan` 结果是独立的内存视图，整理后仍可正常读取；只有不再被任何读取方引用的磁盘数据才会被回收。

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

测试使用一个独立于引擎实现的 `map` 参考模型重放读写历史，并逐批对照引擎快照。范围查询测试用覆盖、删除、空值（含 nil）和相邻键边界（如 `a`、`a\x00`、`aa`、`ab`）对所有区间组合与模型逐项对照，并验证扫描结果在遍历期间发生批量更新、删除时保持开始时的视图。日志整理测试在快照发布与日志切换的每个阶段注入崩溃（临时文件从第 0 字节到完整帧逐字节截断、完整临时文件未 rename、rename 完成后），重开确认不丢已确认数据、不暴露半批提交、不残留临时文件。崩溃样例会构造两条已提交记录和一条未提交批次，然后对未提交帧从第 1 字节到倒数第 1 字节逐字节截断，验证每个写入边界：

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
