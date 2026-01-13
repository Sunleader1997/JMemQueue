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
    public JSharedMemQueue(String topic, int capacity) throws Exception {
        this(topic, capacity, false);
    }

    public JSharedMemQueue(String topic, int capacity, boolean overwrite) throws IOException {
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
            JSharedMemSegment segment = createSegment(); // 当前SMG
            // 如果当前位置空闲，将内存改为写占用，并移动索引
            if (segment.compareAndSetState(JSharedMemSegment.STATE_IDLE, JSharedMemSegment.STATE_WRITING)) {
                this.jSharedMemBaseInfo.increaseTotalOffset(); // 移动索引
                segment.writeContent(data);
                return true;
            }// 如果修改失败说明当前位置已经被占用，需要重新获取 segment
        }
    }


    public JSharedMemSegment createSegment() {
        try {
            return this.writeCarriage.createSegment();
        } catch (CarriageIndexMatchException e) { // 如果满了，则创建新的 TODO 如果jSharedMemCarriage有引用，需要等待引用线程结束
            createWriteCarriage();
            return createSegment();
        }
    }


    public long getTotalOffset() {
        return this.jSharedMemBaseInfo.getTotalOffset();
    }
}
