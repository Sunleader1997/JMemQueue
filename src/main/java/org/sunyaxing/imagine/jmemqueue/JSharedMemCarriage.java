package org.sunyaxing.imagine.jmemqueue;

import org.sunyaxing.imagine.jmemqueue.exceptions.CarriageIndexMatchException;
import org.sunyaxing.imagine.jmemqueue.exceptions.CarriageInitFailException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 车厢: 存储 SEGMENT 顺序集合
 */
public class JSharedMemCarriage implements AutoCloseable {

    private final JSharedMemBaseInfo jSharedMemBaseInfo;
    private final ByteBuffer sharedMemory; // 整个共享内存，存储JSharedMemSegment
    // 当前车厢索引
    private final long currentCarriageIndex;

    private final long capacity;

    /**
     * @param jSharedMemBaseInfo 基础信息
     */
    public JSharedMemCarriage(JSharedMemBaseInfo jSharedMemBaseInfo) {
        this(jSharedMemBaseInfo, jSharedMemBaseInfo.getTotalOffset());
    }

    public JSharedMemCarriage(JSharedMemBaseInfo jSharedMemBaseInfo, long offset) {
        this.jSharedMemBaseInfo = jSharedMemBaseInfo;
        this.capacity = jSharedMemBaseInfo.getCarriage();
        // 链接当前共享内存
        this.currentCarriageIndex = offset / capacity;
        String carriagePath = getCarriagePath(this.currentCarriageIndex);
        try {
            RandomAccessFile accessFile = new RandomAccessFile(carriagePath, "rw");
            FileChannel channel = accessFile.getChannel();
            this.sharedMemory = channel.map(FileChannel.MapMode.READ_WRITE, 0, capacity * JSharedMemSegment.SMG_SIZE);
        } catch (IOException e) {
            throw new CarriageInitFailException();
        }
        System.out.println("【CARRIAGE】LOCATE AT [" + carriagePath + "] OFFSET BEGIN : " + offset);
    }

    public String getCarriagePath(long carriageIndex) {
        return Dictionary.PARENT_DIR + "ipc_" + jSharedMemBaseInfo.getTopic() + ".dat" + "." + carriageIndex;
    }

    public JSharedMemSegment getSegment(long offset) {
        int compare = compareTo(offset);
        if (compare == 0) { // 直接取出数据块
            int index = (int) (offset % capacity);
            return new JSharedMemSegment(sharedMemory, index);
        } else {
            throw new CarriageIndexMatchException("【车厢】当前车厢已过时" + currentCarriageIndex);
        }
    }

    public long getCarriageIndex() {
        return currentCarriageIndex;
    }

    /**
     * 判断offset是否匹配当前车厢
     *
     * @param offset 提供的offset
     * @return -1 当前车厢已经旧了，需要创建新的 0 匹配 1 提供的offset落后了
     */
    public int compareTo(long offset) {
        long carriageIndex = offset / capacity;
        return Long.compare(currentCarriageIndex, carriageIndex);
    }

    @Override
    public void close() {

    }
}
