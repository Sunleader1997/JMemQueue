package org.sunyaxing.imagine.jmemqueue;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
    private final int byteIndex; // 当前SMG的起始偏移量
    /**
     * VarHandle用于对ByteBuffer进行CAS操作
     */
    private static final VarHandle INT_HANDLE = MethodHandles.byteBufferViewVarHandle(
            int[].class,
            ByteOrder.nativeOrder()
    );

    public JSharedMemSegment(ByteBuffer buffer, int index) {
        this.buffer = buffer;
        this.byteIndex = index * SMG_SIZE;
    }

    /**
     * 获取指定位置的状态
     */
    public static int getCurrentState(ByteBuffer buffer, int offset) {
        return (int) INT_HANDLE.getVolatile(buffer, offset + STATE_OFFSET);
    }

    /**
     * 使用CAS方式尝试将状态从expectedState改为newState
     * 可作用于不同进程下对同一个数值的cas操作
     */
    public boolean compareAndSetState(int expectedState, int newState) {
        return INT_HANDLE.compareAndSet(buffer, byteIndex + STATE_OFFSET, expectedState, newState);
    }

    public boolean isState(int state) {
        return getState() == state;
    }

    /**
     * 读取状态
     */
    public int getState() {
        return (int) INT_HANDLE.getVolatile(buffer, byteIndex + STATE_OFFSET);
    }

    /**
     * 设置状态
     */
    public void setState(int newState) {
        INT_HANDLE.setVolatile(buffer, byteIndex + STATE_OFFSET, newState);
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
        if (data.length > MAX_CONTENT_SIZE) {
            throw new IllegalArgumentException("数据大小超过最大限制: " + MAX_CONTENT_SIZE);
        }
        try {
            this.setSize(data.length);
            buffer.put(byteIndex + CONTENT_OFFSET, data);
        } finally {
            this.setState(JSharedMemSegment.STATE_READABLE); // 不管写入是否执行成功，都将状态改为写完成 否则读线程的顺序读取会一直在等待数据可读
        }
    }

    /**
     * 读取数据内容
     */
    public byte[] readContent() {
        try {
            byte[] data = new byte[getSize()];
            buffer.get(byteIndex + CONTENT_OFFSET, data);
            return data;
        } finally {
            setState(JSharedMemSegment.STATE_IDLE);
        }
    }

    /**
     * 获取当前SMG的起始偏移量
     */
    public int getByteIndex() {
        return byteIndex;
    }
}
