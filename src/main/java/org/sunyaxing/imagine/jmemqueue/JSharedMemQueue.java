package org.sunyaxing.imagine.jmemqueue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class JSharedMemQueue {

    private final ByteBuffer sharedMemory; // 整个共享内存
    private final int capacity; // 队列容量（SMG个数）
    /**
     * 读索引（字节偏移量）
     */
    private final AtomicInteger readIndex;
    /**
     * 写索引（字节偏移量）
     */
    private final AtomicInteger writeIndex;

    /**
     * 创建共享内存队列
     *
     * @param fileName MappedByteBuffer 映射地址
     * @param capacity 队列容量（SMG个数）
     */
    public JSharedMemQueue(String fileName, int capacity) throws Exception {
        this(fileName, capacity, false);
    }

    public JSharedMemQueue(String fileName, int capacity, boolean overwrite) throws IOException {
        String parentDir = System.getProperty("java.io.tmpdir") + File.separator + "JSMQ" + File.separator;
        new File(parentDir).mkdir();
        String path = parentDir + "ipc_" + fileName + ".dat";
        File file = new File(path);
        if (overwrite) file.delete();
        System.out.println(path);
        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        FileChannel channel = accessFile.getChannel();
        this.capacity = capacity;
        this.sharedMemory = channel.map(FileChannel.MapMode.READ_WRITE, 0, (long) capacity * JSharedMemSegment.SMG_SIZE);
        this.readIndex = new AtomicInteger(0);
        this.writeIndex = new AtomicInteger(0);
    }

    /**
     * 入队操作 - 写入数据
     *
     * @param data 待写入的数据
     * @return 是否成功写入
     */
    public boolean enqueue(byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("数据不能为空");
        }

        if (data.length > JSharedMemSegment.MAX_CONTENT_SIZE) {
            throw new IllegalArgumentException("数据大小超过最大限制: " + JSharedMemSegment.MAX_CONTENT_SIZE);
        }
        // 遍历环形缓冲区应该在内部执行，如果仅返回false表示插入失败,外部不知道队列是否被占满
        // 尝试写入，最多重试capacity次（遍历整个环形缓冲区）
        for (int attempts = 0; attempts < capacity; attempts++) {
            // 原子地获取并递增写索引
            // 使用 CAS 防止多线程冲突
            int currentIndex = writeIndex.getAndUpdate(old -> (old + JSharedMemSegment.SMG_SIZE) % (capacity * JSharedMemSegment.SMG_SIZE));
            // 读取当前状态
            int state = JSharedMemSegment.getCurrentState(sharedMemory, currentIndex);
            // 如果可写（状态为0）
            if (state == JSharedMemSegment.STATE_IDLE) {
                // 当前SMG
                JSharedMemSegment segment = new JSharedMemSegment(sharedMemory, currentIndex);
                // 使用CAS将状态改为写占用
                if (segment.compareAndSetState(state, JSharedMemSegment.STATE_WRITING)) {
                    try {
                        // 写入数据大小
                        segment.setSize(data.length);
                        // 写入数据内容
                        segment.writeContent(data);
                        // 标记数据写入完成
                        segment.setState(JSharedMemSegment.STATE_READABLE);
                        return true;
                    } catch (Exception e) {
                        // 写入失败，恢复状态
                        segment.setState(state);
                        throw e;
                    }
                }
            }
            // 如果当前位置不可用，继续尝试下一个位置
        }
        return false; // 队列已满
    }

    /**
     * 出队操作 - 读取数据（支持超时等待）
     *
     * @return 读取到的数据，如果队列为空或超时返回null
     */
    public byte[] dequeue() {
        // 遍历一遍环形缓冲区,直到取到数据
        for (int attempts = 0; attempts < capacity; attempts++) {
            int currentIndex = readIndex.getAndUpdate(old -> (old + JSharedMemSegment.SMG_SIZE) % (capacity * JSharedMemSegment.SMG_SIZE));
            int state = JSharedMemSegment.getCurrentState(sharedMemory, currentIndex);
            // 如果可读（状态为2）
            if (state == JSharedMemSegment.STATE_READABLE) {
                JSharedMemSegment segment = new JSharedMemSegment(sharedMemory, currentIndex);
                // 使用CAS将状态改为读占用
                if (segment.compareAndSetState(state, JSharedMemSegment.STATE_READING)) {
                    // 读取状态信息
                    try {
                        // 读取数据大小
                        int size = segment.getSize();
                        // 读取数据内容
                        byte[] content = segment.readContent(size);
                        // 标记数据已读
                        segment.setState(JSharedMemSegment.STATE_IDLE);
                        return content;
                    } catch (Exception e) {
                        segment.setState(state);
                        throw new RuntimeException("读取数据失败", e);
                    }
                }
            }
            // 如果当前位置不可用，继续尝试下一个位置
        }
        // 队列为空
        return null;
    }

    /**
     * 获取队列容量
     */
    public int getCapacity() {
        return capacity;
    }
}
