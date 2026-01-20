package io.github.sunleader1997.jmemqueue;

import io.github.sunleader1997.jmemqueue.exceptions.CarriageInitFailException;
import io.github.sunleader1997.jmemqueue.ttl.TimeToLive;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * 读取器
 * 需要记录读取位置
 */
public class JSharedMemReader implements AutoCloseable {
    private static final long BASE_SIZE = 1024 * 1024;
    private final JSharedMemBaseInfo jSharedMemBaseInfo;
    private final ThreadLocal<JSharedMemCarriage> threadLocalReadCarriage = new ThreadLocal<>();
    private final RandomAccessFile accessFile;
    private final FileChannel channel;
    private final ByteBuffer readerSharedMemory;
    private final String group;
    private final TimeToLive timeToLive;

    private final int INDEX_READER_OFFSET = 0;
    /**
     * VarHandle用于对ByteBuffer进行CAS操作
     */
    private static final VarHandle LONG_HANDLE = MethodHandles.byteBufferViewVarHandle(
            long[].class,
            ByteOrder.nativeOrder()
    );

    /**
     * @param jSharedMemBaseInfo
     * @param group              group 负责负载均衡 同kafka的group
     */
    public JSharedMemReader(JSharedMemBaseInfo jSharedMemBaseInfo, String group, TimeToLive timeToLive) {
        this.group = group;
        this.jSharedMemBaseInfo = jSharedMemBaseInfo;
        this.timeToLive = timeToLive;
        String carriagePath = getReaderPath();
        try {
            this.accessFile = new RandomAccessFile(carriagePath, "rw");
            this.channel = accessFile.getChannel();
            this.readerSharedMemory = channel.map(FileChannel.MapMode.READ_WRITE, 0, BASE_SIZE);
        } catch (IOException e) {
            throw new CarriageInitFailException();
        }
    }

    public JSharedMemCarriage getReadCarriage(long offset) {
        JSharedMemCarriage readCarriage = threadLocalReadCarriage.get();
        if (readCarriage != null) {
            long compare = readCarriage.compareTo(offset);
            if (compare == 0) {
                return readCarriage;
            } else {
                readCarriage.close();
                JSharedMemCarriage newReadCarriage = new JSharedMemCarriage(jSharedMemBaseInfo, offset, timeToLive);
                // 如果映射失败，说明文件丢失
                newReadCarriage.mmap(FileChannel.MapMode.READ_ONLY);
                threadLocalReadCarriage.set(newReadCarriage);
                return newReadCarriage;
            }
        } else {
            JSharedMemCarriage newReadCarriage = new JSharedMemCarriage(jSharedMemBaseInfo, offset, timeToLive);
            newReadCarriage.mmap(FileChannel.MapMode.READ_ONLY);
            threadLocalReadCarriage.set(newReadCarriage);
            return newReadCarriage;
        }
    }

    public long getReaderOffset() {
        return (long) LONG_HANDLE.getVolatile(readerSharedMemory, INDEX_READER_OFFSET);
    }

    /**
     * 重置offset到指定位置
     *
     * @param offset
     */
    public void commitOffset(long offset) {
        LONG_HANDLE.set(readerSharedMemory, INDEX_READER_OFFSET, offset);
    }

    /**
     * 使用 CAS方式尝试将状态从 expectedState 改为 newState
     * 可作用于不同进程下对同一个数值的cas操作
     *
     * @return -1 表示队列已空
     */
    public long getAndIncreaseOffset() {
        while (true) {
            long offset = getReaderOffset();
            if (offset >= jSharedMemBaseInfo.getTotalOffset()) {
                return -1;
            }
            boolean suc = LONG_HANDLE.compareAndSet(readerSharedMemory, INDEX_READER_OFFSET, offset, offset + 1);
            if (suc) return offset; // false 时说明offset被其他线程获取到
        }
    }

    public JSharedMemSegment getReadableSegment() {
        while (true) {
            long offset = getAndIncreaseOffset(); // cas 拉取到offset
            if (offset < 0) return null; // 如果消费队列已空，则返回 null
            JSharedMemCarriage readCarriage = getReadCarriage(offset);
            if (readCarriage.exist()) {
                JSharedMemSegment segment = readCarriage.getSegment(offset);
                if (!segment.isReadable()) { // 如果状态不是可读，则返回 null
                    return null;
                }
                return segment;
            }
        }
    }

    /**
     * 出队操作 - 读取数据（支持超时等待）
     * 查找当前位置，按顺序读取, 没有数据则等待
     *
     * @return 读取到的数据，如果队列为空或超时返回null
     */
    public byte[] dequeue() {
        // getSegment 做 offset increase 时已经保证了 offset 的唯一性
        JSharedMemSegment segment = getReadableSegment();
        if (segment == null) { // 如果队列已空，则返回null
            return null;
        } // 如果有数据，则尝试修改状态为正在读取
        return segment.readContent();
    }

    public String getReaderPath() {
        return Dictionary.PARENT_DIR + jSharedMemBaseInfo.getTopic() + "_" + group + ".reader";
    }

    @Override
    public void close() {
        try {
            System.out.println("【Reader】 执行销毁");
            this.accessFile.close();
            this.channel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
