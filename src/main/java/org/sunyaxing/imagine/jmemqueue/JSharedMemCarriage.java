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
public class JSharedMemCarriage {

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
        System.out.println("CARRIAGE " + carriagePath + " OFFSET : " + offset);
    }

    public String getCarriagePath(long carriageIndex) {
        return Dictionary.PARENT_DIR + "ipc_" + jSharedMemBaseInfo.getTopic() + ".dat" + "." + carriageIndex;
    }

    public JSharedMemSegment getSegment(long offset) {
        long carriageIndex = offset / capacity;
        if (carriageIndex != currentCarriageIndex) {
            throw new CarriageIndexMatchException("当前offset(" + offset + ")对应的车厢索引和当前车厢索引(" + currentCarriageIndex + ")不匹配");
        }
        int index = (int) (offset % capacity);
        return new JSharedMemSegment(sharedMemory, index);
    }
}
