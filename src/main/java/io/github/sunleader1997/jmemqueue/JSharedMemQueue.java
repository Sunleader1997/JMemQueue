package io.github.sunleader1997.jmemqueue;

import io.github.sunleader1997.jmemqueue.ttl.TimeToLive;

import java.nio.channels.FileChannel;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class JSharedMemQueue implements AutoCloseable {

    private final JSharedMemBaseInfo jSharedMemBaseInfo;
    // 每个线程自己维护一个车厢，防止竞态
    private final ThreadLocal<JSharedMemCarriage> threadLocalWriteCarriage = new ThreadLocal<>();

    private final TimeToLive timeToLive;
    public static final int DEF_MSG_SIZE = 1000;

    /**
     * 创建共享内存队列
     *
     * @param topic    MappedByteBuffer 映射地址
     * @param capacity 队列容量（SMG个数）
     */
    public JSharedMemQueue(String topic, int capacity) {
        this(topic, DEF_MSG_SIZE, capacity, false, new TimeToLive(7, TimeUnit.DAYS));
    }
    /**
     * 创建共享内存队列
     *
     * @param topic    MappedByteBuffer 映射地址
     * @param capacity 队列容量（SMG个数）
     */
    public JSharedMemQueue(String topic,int maxMsgSize, int capacity) {
        this(topic, maxMsgSize, capacity, false, new TimeToLive(7, TimeUnit.DAYS));
    }

    /**
     * 创建共享内存队列
     *
     * @param topic      TOPIC
     * @param capacity   每个数据车厢可容纳的数据个数
     * @param overwrite  是否覆盖现有数据从0开始
     * @param timeToLive 数据保留时间（清理时机为创建新的数据车厢时）
     */
    public JSharedMemQueue(String topic, int msgMaxSize, int capacity, boolean overwrite, TimeToLive timeToLive) {
        this.jSharedMemBaseInfo = new JSharedMemBaseInfo(topic, msgMaxSize, capacity, overwrite); // 基础信息
        this.timeToLive = timeToLive;
    }

    /**
     * 创建一个读取器
     * 随机一个group，数据将从0开始消费
     *
     * @return 消费者
     */
    public JSharedMemReader createReader() {
        return createReader(UUID.randomUUID().toString()).needClean();
    }

    /**
     * 创建一个临时读取器
     * 临时 reader 在销毁时清理文件
     *
     * @param group 指定 group 名称，同 kafka 的 group，消息将在 group 内负载均衡
     */
    public JSharedMemReader createReader(String group) {
        return new JSharedMemReader(this.jSharedMemBaseInfo, group, timeToLive);
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
        return this.jSharedMemBaseInfo.getTotalOffset();
    }

    @Override
    public void close() {
        try {
            System.out.println("【QUEUE】 执行销毁");
            this.jSharedMemBaseInfo.close();
            JSharedMemCarriage writeCarriage = threadLocalWriteCarriage.get();
            if (writeCarriage != null) {
                threadLocalWriteCarriage.remove();
                writeCarriage.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
