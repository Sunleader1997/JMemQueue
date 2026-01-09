package org.sunyaxing.imagine.jmemqueue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * 一个SMG
 */
public class JSharedMemSegment {
    /**
     * 单个SMG大小: 1KB
     */
    public static final int SMG_SIZE = 1024;
    /**
     * 状态字段偏移量
     */
    public static final int STATE_OFFSET = 0;
    /**
     * 大小字段偏移量
     */
    public static final int SIZE_OFFSET = 4;
    /**
     * 内容字段偏移量
     */
    public static final int CONTENT_OFFSET = 8;
    /**
     * 最大内容大小
     */
    public static final int MAX_CONTENT_SIZE = SMG_SIZE - CONTENT_OFFSET; // 1024 - CONTENT_OFFSET 字节

    /**
     * 状态：空闲
     */
    public static final int STATE_IDLE = 0;

    /**
     * 状态：写占用
     */
    public static final int STATE_WRITING = 1;

    /**
     * 状态：可读(写完毕)
     */
    public static final int STATE_READABLE = 2;

    /**
     * 状态：读占用
     */
    public static final int STATE_READING = 3;


    private final ByteBuffer buffer; // 整个内存分区
    private final int offset; // 当前SMG的起始偏移量
    /**
     * 使用volatile保证可见性
     */
    private volatile int state;
    /**
     * CAS更新器
     */
    private static final AtomicIntegerFieldUpdater<JSharedMemSegment> STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(JSharedMemSegment.class, "state");

    public JSharedMemSegment(ByteBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
        this.state = buffer.getInt(offset + STATE_OFFSET);
    }

    /**
     * 获取指定位置的状态
     */
    public static int getCurrentState(ByteBuffer buffer, int offset) {
        return buffer.getInt(offset);
    }

    /**
     * 使用CAS方式尝试将状态从expectedState改为newState
     */
    public boolean compareAndSetState(int expectedState, int newState) {
        if (STATE_UPDATER.compareAndSet(this, expectedState, newState)) {
            buffer.putInt(offset + STATE_OFFSET, newState);
            return true;
        }
        return false;
    }

    /**
     * 读取状态
     */
    public int getState() {
        return buffer.getInt(offset);
    }

    /**
     * 设置状态
     */
    public void setState(int newState) {
        this.state = newState;
        buffer.putInt(offset, newState);
    }

    /**
     * 读取数据大小
     */
    public int getSize() {
        return buffer.getInt(offset + SIZE_OFFSET);
    }

    /**
     * 设置数据大小
     */
    public void setSize(int size) {
        buffer.putInt(offset + SIZE_OFFSET, size);
    }

    /**
     * 写入数据内容
     * TODO 如果数据超出最大值，是否应该切换到下一个SMG
     */
    public void writeContent(byte[] data) {
        if (data.length > MAX_CONTENT_SIZE) {
            throw new IllegalArgumentException("数据大小超过最大限制: " + MAX_CONTENT_SIZE);
        }
        buffer.put(offset + CONTENT_OFFSET, data);
    }

    /**
     * 读取数据内容
     */
    public byte[] readContent(int size) {
        if (size > MAX_CONTENT_SIZE) {
            throw new IllegalArgumentException("数据大小超过最大限制: " + MAX_CONTENT_SIZE);
        }
        byte[] data = new byte[size];
        buffer.get(offset + CONTENT_OFFSET, data);
        return data;
    }

    /**
     * 获取当前SMG的起始偏移量
     */
    public int getOffset() {
        return offset;
    }
}
