package io.github.sunleader1997.jmemqueue;

import io.github.sunleader1997.jmemqueue.exceptions.CarriageInitFailException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

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
    // 车厢容量
    private static final int INDEX_CARRIAGE = 8;
    private static final int INDEX_SEGMENT_SIZE = 16;
    private static final int NEXT_RENAME = 20;

    public JSharedMemBaseInfo(String topic, int msgMaxSize, int carriage, boolean overwrite) {
        this.topic = topic;
        Path path = Dictionary.getAndMakeTopicDir(topic).resolve(topic + ".base");
        File file = path.toFile();
        try {
            this.accessFile = new RandomAccessFile(file, "rw");
            this.channel = accessFile.getChannel();
            this.sharedBaseMemory = this.channel.map(FileChannel.MapMode.READ_WRITE, 0, BASE_SIZE);
        } catch (IOException e) {
            throw new CarriageInitFailException(e);
        }
        this.setCarriage(carriage);
        this.setMsgMaxSize(msgMaxSize);
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

    public int getMsgMaxSize() {
        return AtomicVarHandle.getInt(sharedBaseMemory, INDEX_SEGMENT_SIZE);
    }

    public void setMsgMaxSize(int msgMaxSize) {
        AtomicVarHandle.setInt(sharedBaseMemory, INDEX_SEGMENT_SIZE, msgMaxSize);
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
