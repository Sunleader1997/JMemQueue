package org.sunyaxing.imagine.jmemqueue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class JSharedMemQueue implements Closeable {

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
    private final FileChannel channel;

    /**
     * 创建共享内存队列
     *
     * @param topic    MappedByteBuffer 映射地址
     * @param capacity 队列容量（SMG个数）
     */
    public JSharedMemQueue(String topic, int capacity) throws Exception {
        this(topic, capacity, false);
    }

    public JSharedMemQueue(String topic, int capacity, boolean overwrite) throws IOException {
        String parentDir = System.getProperty("java.io.tmpdir") + File.separator + "JSMQ" + File.separator;
        new File(parentDir).mkdir();
        String path = parentDir + "ipc_" + topic + ".dat";
        File file = new File(path);
        if (overwrite) file.delete();
        System.out.println(path);
        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        this.channel = accessFile.getChannel();
        this.capacity = capacity;
        this.sharedMemory = this.channel.map(FileChannel.MapMode.READ_WRITE, 0, (long) capacity * JSharedMemSegment.SMG_SIZE);
        this.readIndex = new AtomicInteger(0);
        this.writeIndex = new AtomicInteger(0);
    }

    /**
     * 入队操作 - 写入数据
     * 写数据，如果当前位置不可用则找下一个位置
     * 队列满则返回 false
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
            // 使用 CAS 原子地获取并递增写索引 防止多线程冲突
            int currentIndex = writeIndex.getAndUpdate(old -> (old + JSharedMemSegment.SMG_SIZE) % (capacity * JSharedMemSegment.SMG_SIZE));
            // 当前SMG
            JSharedMemSegment segment = new JSharedMemSegment(sharedMemory, currentIndex);
            // 如果当前位置空闲，使用CAS将状态改为写占用
            if (segment.compareAndSetState(JSharedMemSegment.STATE_IDLE, JSharedMemSegment.STATE_WRITING)) {
                try {
                    // 写入数据大小
                    segment.setSize(data.length);
                    // 写入数据内容
                    segment.writeContent(data); // 如果进程崩溃，当前位置将处于写占用状态，如何优化？
                    // 标记数据写入完成
                    segment.setState(JSharedMemSegment.STATE_READABLE);
                    return true;
                } catch (Exception e) {
                    segment.setState(JSharedMemSegment.STATE_IDLE);
                    throw e;
                }
            }
            // 如果当前位置不可用，继续尝试下一个位置
        }
        return false;
    }

    public byte[] dequeue() {
        return dequeue(1);
    }

    /**
     * 出队操作 - 读取数据（支持超时等待）
     * 查找当前位置，按顺序读取, 没有数据则等待
     *
     * @return 读取到的数据，如果队列为空或超时返回null
     */
    public byte[] dequeue(long timeoutMs) {
        // 顺序读取
        int currentIndex = readIndex.getAndUpdate(old -> (old + JSharedMemSegment.SMG_SIZE) % (capacity * JSharedMemSegment.SMG_SIZE));
        JSharedMemSegment segment = new JSharedMemSegment(sharedMemory, currentIndex);
        long startTime = System.currentTimeMillis();
        boolean timeout = false;
        // 循环当前位置状态，直到找到可读状态，并修改为读取占用
        while (true) {
            if (segment.compareAndSetState(JSharedMemSegment.STATE_READABLE, JSharedMemSegment.STATE_READING)) {
                try {
                    // 读取数据大小
                    int size = segment.getSize();
                    // 读取数据内容
                    byte[] content = segment.readContent(size);
                    // 标记数据已读
                    segment.setState(JSharedMemSegment.STATE_IDLE);
                    return content;
                } catch (Exception e) {
                    segment.setState(JSharedMemSegment.STATE_READABLE);
                    throw e;
                }
            }
            // 如果超时就挂起，降低 CPU 空转
            if (timeout || (timeoutMs > 0 && System.currentTimeMillis() - startTime > timeoutMs)) {
                try {
                    Thread.sleep(timeoutMs);
                    timeout = true;
                } catch (InterruptedException e) {
                }
            }
        }
    }

    /**
     * 获取队列容量
     */
    public int getCapacity() {
        return capacity;
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }
}
