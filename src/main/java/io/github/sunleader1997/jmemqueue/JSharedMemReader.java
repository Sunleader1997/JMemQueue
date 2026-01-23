package io.github.sunleader1997.jmemqueue;

import io.github.sunleader1997.jmemqueue.exceptions.CarriageInitFailException;
import io.github.sunleader1997.jmemqueue.ttl.JCleaner;
import io.github.sunleader1997.jmemqueue.ttl.TimeToLive;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 读取器
 * 需要记录读取位置
 */
public class JSharedMemReader implements AutoCloseable {
    private static final long BASE_SIZE = 1024 * 1024;
    private final ThreadLocal<JSharedMemCarriage> threadLocalReadCarriage = new ThreadLocal<>();
    private final JSharedMemBaseInfo jSharedMemBaseInfo;
    private final String group;
    private final File readerFile;
    private boolean needClean = false;
    private TimeToLive timeToLive;

    private RandomAccessFile accessFile;
    private FileChannel channel;
    private MappedByteBuffer readerSharedMemory;

    private final int INDEX_READER_OFFSET = 0;

    /**
     * 创建默认的消费者
     * 随机group，从头消费
     *
     * @param topic
     */
    public JSharedMemReader(String topic) {
        this(topic, UUID.randomUUID().toString());
        needClean();
    }

    public JSharedMemReader(String topic, String group) {
        this.jSharedMemBaseInfo = new JSharedMemBaseInfo(topic, 0, 0, false); // 基础信息
        this.jSharedMemBaseInfo.mmap(FileChannel.MapMode.READ_ONLY); // 读模式
        this.jSharedMemBaseInfo.print();
        this.group = group;
        Path carriagePath = getReaderPath();
        this.readerFile = carriagePath.toFile();
        this.mmap();
    }

    /**
     * 挂载数据
     */
    public void mmap() {
        try {
            this.accessFile = new RandomAccessFile(this.readerFile, "rw");
            this.channel = accessFile.getChannel();
            this.readerSharedMemory = channel.map(FileChannel.MapMode.READ_WRITE, 0, BASE_SIZE);
        } catch (IOException e) {
            throw new CarriageInitFailException();
        }
    }

    /**
     * 出队操作 - 读取数据（支持超时等待）
     * 查找当前位置，按顺序读取, 没有数据则等待
     *
     * @return 读取到的数据，如果队列为空或超时返回null
     */
    public byte[] dequeue() {
        if (this.jSharedMemBaseInfo.isMapped()) {
            // getSegment 做 offset increase 时已经保证了 offset 的唯一性
            JSharedMemSegment segment = getReadableSegment();
            if (segment == null) { // 如果队列已空，则返回null
                return null;
            } // 如果有数据，则尝试修改状态为正在读取
            return segment.readContent();
        } else {
            this.jSharedMemBaseInfo.mmap(FileChannel.MapMode.READ_ONLY);
            return null;
        }
    }

    public JSharedMemSegment getReadableSegment() {
        while (true) {
            long offset = getAndIncreaseOffset(); // cas 拉取到offset
            if (offset < 0) return null; // 如果消费队列已空，则返回 null
            JSharedMemCarriage readCarriage = getReadCarriage(offset);
            if (readCarriage.exist()) {
                JSharedMemSegment segment = readCarriage.getSegment(offset);
                if (!segment.isReadable()) { // 如果状态不是可读，则返回 null
                    return null;
                }
                return segment;
            }
        }
    }

    /**
     * 使用 CAS方式尝试将状态从 expectedState 改为 newState
     * 可作用于不同进程下对同一个数值的cas操作
     *
     * @return -1 表示队列已空
     */
    public long getAndIncreaseOffset() {
        while (true) {
            long offset = getReaderOffset();
            if (offset >= jSharedMemBaseInfo.readTotalOffset()) {
                return -1;
            }
            boolean suc = AtomicVarHandle.compareAndSetLong(readerSharedMemory, INDEX_READER_OFFSET, offset, offset + 1);
            if (suc) return offset; // false 时说明offset被其他线程获取到
        }
    }

    public JSharedMemCarriage getReadCarriage(long offset) {
        JSharedMemCarriage readCarriage = getCurrentCarriage();
        if (readCarriage != null) {
            long compare = readCarriage.compareTo(offset);
            if (compare == 0) {
                return readCarriage;
            } else {
                threadLocalReadCarriage.remove();
                readCarriage.close();
                JSharedMemCarriage newReadCarriage = new JSharedMemCarriage(jSharedMemBaseInfo, offset, timeToLive, FileChannel.MapMode.READ_ONLY);
                threadLocalReadCarriage.set(newReadCarriage);
                return newReadCarriage;
            }
        } else {
            JSharedMemCarriage newReadCarriage = new JSharedMemCarriage(jSharedMemBaseInfo, offset, timeToLive, FileChannel.MapMode.READ_ONLY);
            threadLocalReadCarriage.set(newReadCarriage);
            return newReadCarriage;
        }
    }

    public long getReaderOffset() {
        return AtomicVarHandle.getLong(readerSharedMemory, INDEX_READER_OFFSET);
    }

    public JSharedMemCarriage getCurrentCarriage() {
        return threadLocalReadCarriage.get();
    }

    public Path getReaderPath() {
        return Dictionary.getTopicDir(jSharedMemBaseInfo.getTopic()).resolve(group + ".reader");
    }

    public void needClean() {
        this.needClean = true;
    }

    public void setTimeToLive(long timeAlive, TimeUnit timeUnit) {
        this.timeToLive = new TimeToLive(timeAlive, timeUnit);
    }

    public void print(){
        this.jSharedMemBaseInfo.print();
    }


    @Override
    public void close() {
        try {
            System.out.println("【Reader】 执行销毁");
            this.threadLocalReadCarriage.remove();
            if (this.readerSharedMemory != null) {
                this.readerSharedMemory.force();
            }
            if (this.accessFile != null) {
                this.accessFile.close();
            }
            if (this.channel != null) {
                this.channel.close();
            }
            if (this.needClean) {
                JCleaner.clean(this.readerSharedMemory);
                boolean remove = this.readerFile.delete();
                System.out.println("DELETE READER: " + this.readerFile.getName() + " STATUS " + remove);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
