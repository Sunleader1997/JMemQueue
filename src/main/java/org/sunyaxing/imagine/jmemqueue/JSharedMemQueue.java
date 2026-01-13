package org.sunyaxing.imagine.jmemqueue;

public class JSharedMemQueue {

    private final JSharedMemBaseInfo jSharedMemBaseInfo;
    // 每个线程自己维护一个车厢，防止竞态
    private final ThreadLocal<JSharedMemCarriage> threadLocalWriteCarriage = new ThreadLocal<>();

    /**
     * 创建共享内存队列
     *
     * @param topic    MappedByteBuffer 映射地址
     * @param capacity 队列容量（SMG个数）
     */
    public JSharedMemQueue(String topic, int capacity) {
        this(topic, capacity, false);
    }

    public JSharedMemQueue(String topic, int capacity, boolean overwrite) {
        this.jSharedMemBaseInfo = new JSharedMemBaseInfo(topic, capacity); // 基础信息
    }

    public JSharedMemReader createReader() {
        return new JSharedMemReader(this.jSharedMemBaseInfo);
    }

    /**
     * 向车厢塞入数据
     */
    public boolean enqueue(byte[] data) {
        while (true) {
            long offset = this.jSharedMemBaseInfo.getAndIncreaseTotalOffset();
            JSharedMemSegment segment = createSegment(offset); // 当前SMG
            // 如果当前位置空闲，将内存改为写占用
            if (segment.compareAndSetState(JSharedMemSegment.STATE_IDLE, JSharedMemSegment.STATE_WRITING)) {
                segment.writeContent(data);
                return true;
            }// 如果修改失败说明当前位置已经被占用，需要重新获取 segment
        }
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
                writeCarriage.close(); // 旧的车厢应该销毁
                JSharedMemCarriage newWriteCarriage = new JSharedMemCarriage(jSharedMemBaseInfo, offset);
                threadLocalWriteCarriage.set(newWriteCarriage);
                if (compare > 0) System.out.println("!!! 方法调用有严重问题");
                return newWriteCarriage;
            }
        } else {
            JSharedMemCarriage newWriteCarriage = new JSharedMemCarriage(jSharedMemBaseInfo, offset);
            threadLocalWriteCarriage.set(newWriteCarriage);
            return newWriteCarriage;
        }
    }


    public long getTotalOffset() {
        return this.jSharedMemBaseInfo.getTotalOffset();
    }
}
