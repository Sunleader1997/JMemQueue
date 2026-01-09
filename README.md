使用共享内存的数据队列，例如 MappedByteBuffer
实现同一台设备内的 IPC 通信

申请一个 1GB 的内存存储 以下的数据结构

> smg 单个协议数据 如下

| 0-3   | 4-7  | 8 - 1023 |
|-------|------|----------|
| state | size | content  |

- 读写状态: 0: 空闲, 1: 写占用 2: 可读(写完毕) 3: 已读

> readCommit() 轮询到下一个smg索引 readIndex += 1024
> writeCommit() 轮询到下一个smg索引 writeIndex += 1024

> Dequeue 线程每次按以下方式读取数据

```javascript
currentIndex = readIndex // 开始索引
wait_until(read(currentIndex, 4) == 2) // 阻塞直到可读 或者 readCommit()
currentIndex += 4
size = read(currentIndex, 8) // 读取数据大小 currentIndex
currentIndex += 8
content = read(currentIndex, size) // 读取数据内容 currentIndex
write(startIndex, 3) // 标记数据已读
readCommit()
```

> Enqueue 线程每次按以下方式写入数据

```javascript
bytes // 待写入数据
currentIndex = writeIndex // 开始索引
state = read(currentIndex, 4) // 读取数据头 currentIndex 使用 cas 将 state 改为1
if (state == 0 || state == 3) { // 如果可写
    write(startIndex, 1) // 标记数据写占用
    currentIndex += 4
    size = bytes.length
    write(currentIndex, size) // 写入数据大小
    currentIndex += 8
    write(currentIndex, bytes) // 写入数据
    write(startIndex, 2) // 标记数据写入完成
}
writeCommit()

```