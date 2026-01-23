package io.github.sunleader1997.jmemqueue;

import io.github.sunleader1997.jmemqueue.ttl.TimeToLive;

import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

public class JSharedMemProducer implements AutoCloseable {
    public static final int DEF_MSG_SIZE = 1000; // 默认生产单个数据不大于1KB
    public static final int DEF_CAPACITY = 1024 * 1024;// 默认车厢承载 1024*1024 条数据（1GB）
    // 每个线程自己维护一个车厢，防止竞态
    private final ThreadLocal<JSharedMemCarriage> threadLocalWriteCarriage = new ThreadLocal<>();

    private final JSharedMemBaseInfo jSharedMemBaseInfo;
    private TimeToLive timeToLive;

    public JSharedMemProducer(String topic) {
        this(topic, DEF_MSG_SIZE, DEF_CAPACITY, false);
    }

    public JSharedMemProducer(String topic, int msgMaxSize, int capacity, boolean overwrite) {
        this.jSharedMemBaseInfo = new JSharedMemBaseInfo(topic, msgMaxSize, capacity, overwrite); // 基础信息
        this.jSharedMemBaseInfo.mmap(FileChannel.MapMode.READ_WRITE); // 读写模式
        this.jSharedMemBaseInfo.flush(); // 写入磁盘
    }

    /**
     * 向车厢塞入数据
     */
    public boolean enqueue(byte[] data) {
        // 这里使用 cas 已经保证 offset 唯一性了，所以可以直接覆盖
        long offset = this.jSharedMemBaseInfo.getAndIncreaseTotalOffset();
        JSharedMemSegment segment = createSegment(offset); // 当前SMG
        segment.writeContent(data);
        return true;
    }

    // 获取当前线程的车厢
    public JSharedMemSegment createSegment(long offset) {
        JSharedMemCarriage writeCarriage = getCarriageForLocal(offset);
        return writeCarriage.getSegment(offset);
    }

    /**
     * 此方法能保证拿到正确的车厢
     */
    public JSharedMemCarriage getCarriageForLocal(long offset) {
        JSharedMemCarriage writeCarriage = threadLocalWriteCarriage.get();
        if (writeCarriage != null) {
            long compare = writeCarriage.compareTo(offset);
            if (compare == 0) {
                return writeCarriage;
            } else {
                threadLocalWriteCarriage.remove();
                writeCarriage.close(); // 旧的车厢应该销毁
                JSharedMemCarriage newWriteCarriage = new JSharedMemCarriage(jSharedMemBaseInfo, offset, timeToLive, FileChannel.MapMode.READ_WRITE);
                threadLocalWriteCarriage.set(newWriteCarriage);
                if (compare > 0) System.out.println("!!! 方法调用有严重问题");
                return newWriteCarriage;
            }
        } else {
            JSharedMemCarriage newWriteCarriage = new JSharedMemCarriage(jSharedMemBaseInfo, offset, timeToLive, FileChannel.MapMode.READ_WRITE);
            threadLocalWriteCarriage.set(newWriteCarriage);
            return newWriteCarriage;
        }
    }

    public long getTotalOffset() {
        return this.jSharedMemBaseInfo.readTotalOffset();
    }

    public void setTimeToLive(long timeAlive, TimeUnit timeUnit) {
        this.timeToLive = new TimeToLive(timeAlive, timeUnit);
    }

    @Override
    public void close() throws Exception {
        JSharedMemCarriage writeCarriage = threadLocalWriteCarriage.get();
        if (writeCarriage != null) {
            threadLocalWriteCarriage.remove();
            writeCarriage.close();
        }
        if (jSharedMemBaseInfo != null) {
            jSharedMemBaseInfo.close();
        }
    }
}
