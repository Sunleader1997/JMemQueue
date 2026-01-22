package io.github.sunleader1997.jmemqueue;

import java.nio.ByteBuffer;

/**
 * 一个SMG
 */
public class JSharedMemSegment {
    /**
     * 单个SMG大小: 1KB
     * 如果不固定单个数据元大小的话，就无法多线程enqueue，dequeue
     */
    public final int smgSize;
    public final int maxContentSize;
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
     * 状态：空闲
     */
    public static final int STATE_IDLE = 0;

    /**
     * 状态：可读(写完毕)
     */
    public static final int STATE_READABLE = 2;


    private final ByteBuffer buffer; // 整个内存分区
    private final int byteIndex; // 当前SMG的起始偏移量

    /**
     *
     * @param buffer  carriage 的 ByteBuffer
     * @param smgSize 单个数据元的容量，同一个Carriage里的size必须一致
     * @param index   索引
     */
    public JSharedMemSegment(ByteBuffer buffer, int smgSize, int index) {
        this.buffer = buffer;
        this.smgSize = smgSize;
        this.maxContentSize = smgSize - CONTENT_OFFSET;
        this.byteIndex = index * smgSize;
    }

    /**
     * 获取指定位置的状态
     */
    public static int getCurrentState(ByteBuffer buffer, int offset) {
        return AtomicVarHandle.getInt(buffer, offset + STATE_OFFSET);
    }

    /**
     * 使用CAS方式尝试将状态从expectedState改为newState
     * 可作用于不同进程下对同一个数值的cas操作
     */
    public boolean compareAndSetState(int expectedState, int newState) {
        return AtomicVarHandle.compareAndSetInt(buffer, byteIndex + STATE_OFFSET, expectedState, newState);
    }

    public boolean isState(int state) {
        return getState() == state;
    }

    /**
     * 读取状态
     */
    public int getState() {
        return AtomicVarHandle.getInt(buffer, byteIndex + STATE_OFFSET);
    }

    /**
     * 设置状态
     */
    public void setState(int newState) {
        AtomicVarHandle.setInt(buffer, byteIndex + STATE_OFFSET, newState);
    }

    /**
     * 读取数据大小
     */
    public int getSize() {
        return buffer.getInt(byteIndex + SIZE_OFFSET);
    }

    /**
     * 设置数据大小
     */
    public void setSize(int size) {
        buffer.putInt(byteIndex + SIZE_OFFSET, size);
    }

    /**
     * 写入数据内容
     * TODO 如果数据超出最大值，是否应该切换到下一个SMG
     */
    public void writeContent(byte[] data) {
        if (data.length > maxContentSize) {
            throw new IllegalArgumentException("数据大小超过最大限制: " + maxContentSize);
        }
        this.setSize(data.length);
        buffer.put(byteIndex + CONTENT_OFFSET, data);
        setState(STATE_READABLE);// 标记当前为可读状态
    }

    /**
     * 读取数据内容
     */
    public byte[] readContent() {
        byte[] data = new byte[getSize()];
        buffer.get(byteIndex + CONTENT_OFFSET, data);
        return data;
    }

    public boolean isReadable() {
        return isState(JSharedMemSegment.STATE_READABLE);
    }

    /**
     * 获取当前SMG的起始偏移量
     */
    public int getByteIndex() {
        return byteIndex;
    }
}
