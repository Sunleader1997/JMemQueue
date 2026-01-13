package org.sunyaxing.imagine.jmemqueue;

import org.sunyaxing.imagine.jmemqueue.exceptions.CarriageIndexMatchException;

import java.io.IOException;

public class JSharedMemQueue {

    private final JSharedMemBaseInfo jSharedMemBaseInfo;
    private JSharedMemCarriage writeCarriage;

    /**
     * 创建共享内存队列
     *
     * @param topic    MappedByteBuffer 映射地址
     * @param capacity 队列容量（SMG个数）
     */
    public JSharedMemQueue(String topic, int capacity) {
        this(topic, capacity, false);
    }

    public JSharedMemQueue(String topic, int capacity, boolean overwrite){
        this.jSharedMemBaseInfo = new JSharedMemBaseInfo(topic, capacity); // 基础信息
    }

    public JSharedMemReader createReader() {
        return new JSharedMemReader(this.jSharedMemBaseInfo);
    }

    public synchronized void createWriteCarriage() {
        this.writeCarriage = new JSharedMemCarriage(this.jSharedMemBaseInfo);
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
        try {
            return this.writeCarriage.getSegment(offset);
        } catch (CarriageIndexMatchException e) { // 如果满了，则创建新的 TODO 如果jSharedMemCarriage有引用，需要等待引用线程结束
            createWriteCarriage();
            return createSegment(offset);
        }
    }


    public long getTotalOffset() {
        return this.jSharedMemBaseInfo.getTotalOffset();
    }
}
