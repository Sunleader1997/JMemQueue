package org.sunyaxing.imagine.jmemqueue;

import org.sunyaxing.imagine.jmemqueue.exceptions.CarriageInitFailException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 队列基础信息
 * 0. 队列总偏移量
 * 1. 车厢容量
 */
public class JSharedMemBaseInfo implements AutoCloseable {
    private static final long BASE_SIZE = 1024 * 1024;

    private final String topic;
    private final MappedByteBuffer sharedBaseMemory;// 存储队列基础信息
    private final FileChannel channel;
    private final RandomAccessFile accessFile;
    // 偏移量的索引开始位置 long 数据，占8位
    private static final int INDEX_TOTAL_OFFSET = 0;
    private static final int INDEX_CARRIAGE = 8;

    public JSharedMemBaseInfo(String topic, int carriage, boolean overwrite) {
        this.topic = topic;
        String path = Dictionary.PARENT_DIR + "ipc_" + topic + ".base";
        File file = new File(path);
        try {
            this.accessFile = new RandomAccessFile(file, "rw");
            this.channel = accessFile.getChannel();
            this.sharedBaseMemory = this.channel.map(FileChannel.MapMode.READ_WRITE, 0, BASE_SIZE);
        } catch (IOException e) {
            throw new CarriageInitFailException(e);
        }
        this.setCarriage(carriage);
        if (overwrite) {
            this.setTotalOffset(0L);
        }
    }

    /**
     * VarHandle用于对ByteBuffer进行CAS操作
     */
    private static final VarHandle LONG_HANDLE = MethodHandles.byteBufferViewVarHandle(long[].class, ByteOrder.nativeOrder());

    /**
     * 获取最新的偏移量
     */
    public long getTotalOffset() {
        return (long) LONG_HANDLE.getVolatile(sharedBaseMemory, INDEX_TOTAL_OFFSET);
    }

    private void setTotalOffset(long offset) {
        LONG_HANDLE.set(sharedBaseMemory, INDEX_TOTAL_OFFSET, offset);
    }

    public long getCarriage() {
        return (long) LONG_HANDLE.getVolatile(sharedBaseMemory, INDEX_CARRIAGE);
    }

    public void setCarriage(long carriage) {
        LONG_HANDLE.set(sharedBaseMemory, INDEX_CARRIAGE, carriage);
    }

    public long getAndIncreaseTotalOffset() {
        while (true) {
            long cto = getTotalOffset();
            if (LONG_HANDLE.compareAndSet(sharedBaseMemory, INDEX_TOTAL_OFFSET, cto, cto + 1)) {
                return cto;
            }
        }
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public void close() {
        try {
            System.out.println("【BaseInfo】 执行销毁");
            this.accessFile.close();
            this.channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
