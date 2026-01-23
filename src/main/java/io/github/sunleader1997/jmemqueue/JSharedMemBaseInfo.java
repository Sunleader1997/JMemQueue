package io.github.sunleader1997.jmemqueue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
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
    private final int msgMaxSize;
    private final int carriage;
    private final File file;
    private FileChannel.MapMode mapMode;

    // 偏移量的索引开始位置 long 数据，占8位
    private static final int INDEX_TOTAL_OFFSET = 0;
    // 车厢容量
    private static final int INDEX_CARRIAGE = 8;
    private static final int INDEX_SEGMENT_SIZE = 16;
    private static final int NEXT_RENAME = 20;

    private MappedByteBuffer sharedBaseMemory;// 存储队列基础信息
    private FileChannel channel;
    private RandomAccessFile accessFile;
    private boolean mapped; // 是否挂载成功

    /**
     * 创建基础信息映射，暂时还没写入
     */
    public JSharedMemBaseInfo(String topic, int msgMaxSize, int carriage, boolean overwrite) {
        this.topic = topic;
        this.msgMaxSize = msgMaxSize;
        this.carriage = carriage;
        Path path = Dictionary.getAndMakeTopicDir(topic).resolve(topic + ".base");
        this.file = path.toFile();
        if (overwrite) {
            this.file.delete();
        }
    }

    /**
     * 挂载数据
     */
    public void mmap(FileChannel.MapMode mode) {
        try {
            this.mapMode = mode;
            this.accessFile = new RandomAccessFile(file, FileChannel.MapMode.READ_WRITE.equals(mode) ? "rw" : "r");
            this.channel = accessFile.getChannel();
            this.sharedBaseMemory = this.channel.map(mode, 0, BASE_SIZE);
            this.mapped = true;
        } catch (IOException e) {
            System.out.println("数据挂载失败");
            this.mapped = false;
        }
    }

    public void print() {
        // 打印基础信息
        if (mapped) {
            System.out.println("\n========== 队列基础信息 ==========");
            System.out.println("状态: " + this.mapMode);
            System.out.println("TOPIC: " + this.topic);
            System.out.println("当前OFFSET: " + this.readTotalOffset());
            System.out.println("单车厢容量: " + this.readCarriage());
            System.out.println("数据元容量: " + this.readMsgMaxSize() + "B");
            System.out.println("===================================");
        }
    }

    /**
     * 刷新到磁盘
     * 如果修改了存储方式，必须删除或者 override
     */
    public void flush() {
        if (this.readCarriage() == 0) {
            this.resetCarriage(carriage);
        }
        if (this.readMsgMaxSize() == 0) {
            this.resetMsgMaxSize(msgMaxSize);
        }
    }

    /**
     * 获取最新的偏移量
     */
    public long readTotalOffset() {
        return AtomicVarHandle.getLong(sharedBaseMemory, INDEX_TOTAL_OFFSET);
    }

    public long readCarriage() {
        return AtomicVarHandle.getLong(sharedBaseMemory, INDEX_CARRIAGE);
    }

    public void resetCarriage(long carriage) {
        AtomicVarHandle.setLong(sharedBaseMemory, INDEX_CARRIAGE, carriage);
    }

    public int readMsgMaxSize() {
        return AtomicVarHandle.getInt(sharedBaseMemory, INDEX_SEGMENT_SIZE);
    }

    public void resetMsgMaxSize(int msgMaxSize) {
        AtomicVarHandle.setInt(sharedBaseMemory, INDEX_SEGMENT_SIZE, msgMaxSize);
    }

    public long getAndIncreaseTotalOffset() {
        while (true) {
            long cto = readTotalOffset();
            if (AtomicVarHandle.compareAndSetLong(sharedBaseMemory, INDEX_TOTAL_OFFSET, cto, cto + 1)) {
                return cto;
            }
        }
    }

    public String getTopic() {
        return topic;
    }

    public boolean isMapped() {
        return mapped;
    }

    @Override
    public void close() {
        try {
            System.out.println("【BaseInfo】 执行销毁");
            this.accessFile.close();
            this.channel.close();
            this.print();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
