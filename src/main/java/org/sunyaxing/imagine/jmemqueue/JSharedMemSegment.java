package org.sunyaxing.imagine.jmemqueue;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * 一个SMG
 * <p>
 * TODO 需要加入下一个数据的索引
 */
public class JSharedMemSegment {
    /**
     * 单个SMG大小: 1KB
     */
    public static final short SMG_SIZE = 1024;
    /**
     * 状态字段偏移量
     */
    public static final byte STATE_OFFSET = 0; // 用1个字节表示状态已经足够 byte
    /**
     * 大小字段偏移量
     */
    public static final byte SIZE_OFFSET = 1; // 用4个字节表示数据大小 int
    /**
     * 内容字段偏移量
     */
    public static final byte CONTENT_OFFSET = 5; // 用接下来的所有字节表示数据内容
    /**
     * 最大内容大小
     */
    public static final short MAX_CONTENT_SIZE = SMG_SIZE - CONTENT_OFFSET; // 1024 - CONTENT_OFFSET 字节

    /**
     * 状态：空闲
     */
    public static final byte STATE_IDLE = 0;

    /**
     * 状态：写占用
     */
    public static final byte STATE_WRITING = 1;

    /**
     * 状态：可读(写完毕)
     */
    public static final byte STATE_READABLE = 2;

    /**
     * 状态：读占用
     */
    public static final byte STATE_READING = 3;


    private final ByteBuffer buffer; // 整个内存分区
    private final int offset; // 当前SMG的起始偏移量
    /**
     * 使用volatile保证可见性
     */
    private volatile byte state;
    /**
     * CAS更新器
     */
    private static final AtomicIntegerFieldUpdater<JSharedMemSegment> STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(JSharedMemSegment.class, "state");

    public JSharedMemSegment(ByteBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
        this.state = buffer.get(offset + STATE_OFFSET);
    }

    /**
     * 使用CAS方式尝试将状态从expectedState改为newState
     */
    public boolean compareAndSetState(int expectedState, byte newState) {
        if (STATE_UPDATER.compareAndSet(this, expectedState, newState)) {
            buffer.put(offset + STATE_OFFSET, newState);
            return true;
        }
        return false;
    }

    /**
     * 读取状态
     */
    public byte getState() {
        return buffer.get(offset);
    }

    /**
     * 设置状态
     */
    public void setState(byte newState) {
        this.state = newState;
        buffer.put(offset, newState);
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
        buffer.position(offset + CONTENT_OFFSET);
        buffer.put(data);
    }

    /**
     * 读取数据内容
     */
    public byte[] readContent(int size) {
        if (size > MAX_CONTENT_SIZE) {
            throw new IllegalArgumentException("数据大小超过最大限制: " + MAX_CONTENT_SIZE);
        }
        byte[] data = new byte[size];
        buffer.position(offset + CONTENT_OFFSET);
        buffer.get(data);
        return data;
    }

    /**
     * 获取当前SMG的起始偏移量
     */
    public int getOffset() {
        return offset;
    }
}
