package org.sunyaxing.imagine.jmemqueue;

import org.sunyaxing.imagine.jmemqueue.exceptions.CarriageInitFailException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

/**
 * 读取器
 * 需要记录读取位置
 */
public class JSharedMemReader implements AutoCloseable{
    private static final long BASE_SIZE = 1024 * 1024;
    private final JSharedMemBaseInfo jSharedMemBaseInfo;
    private final ThreadLocal<JSharedMemCarriage> threadLocalReadCarriage = new ThreadLocal<>();
    private final RandomAccessFile accessFile;
    private final FileChannel channel;
    private final ByteBuffer readerSharedMemory;

    private final int INDEX_READER_OFFSET = 0;
    /**
     * VarHandle用于对ByteBuffer进行CAS操作
     */
    private static final VarHandle LONG_HANDLE = MethodHandles.byteBufferViewVarHandle(
            long[].class,
            ByteOrder.nativeOrder()
    );

    public JSharedMemReader(JSharedMemBaseInfo jSharedMemBaseInfo) {
        this.jSharedMemBaseInfo = jSharedMemBaseInfo;
        String carriagePath = getReaderPath();
        try {
            this.accessFile = new RandomAccessFile(carriagePath, "rw");
            this.channel = accessFile.getChannel();
            this.readerSharedMemory = channel.map(FileChannel.MapMode.READ_WRITE, 0, BASE_SIZE);
            long readOffset = getReaderOffset();// 恢复读取位置
            getReadCarriage(readOffset);
        } catch (IOException e) {
            throw new CarriageInitFailException();
        }
    }

    public JSharedMemCarriage getReadCarriage(long offset) {
        JSharedMemCarriage readCarriage = threadLocalReadCarriage.get();
        if (readCarriage != null) {
            long compare = readCarriage.compareTo(offset);
            if (compare == 0) {
                return readCarriage;
            } else {
                readCarriage.close();
                JSharedMemCarriage newReadCarriage = new JSharedMemCarriage(jSharedMemBaseInfo, offset);
                threadLocalReadCarriage.set(newReadCarriage);
                return newReadCarriage;
            }
        } else {
            JSharedMemCarriage newReadCarriage = new JSharedMemCarriage(jSharedMemBaseInfo, offset);
            threadLocalReadCarriage.set(newReadCarriage);
            return newReadCarriage;
        }
    }

    public long getReaderOffset() {
        return (long) LONG_HANDLE.getVolatile(readerSharedMemory, INDEX_READER_OFFSET);
    }

    public void commitOffset(long offset) {
        LONG_HANDLE.set(readerSharedMemory, INDEX_READER_OFFSET, offset);
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
            if (offset >= jSharedMemBaseInfo.getTotalOffset()) {
                return -1;
            }
            boolean suc = LONG_HANDLE.compareAndSet(readerSharedMemory, INDEX_READER_OFFSET, offset, offset + 1);
            if (suc) return offset; // false 时说明offset被其他线程获取到
        }
    }

    public JSharedMemSegment getSegment() {
        long offset = getAndIncreaseOffset(); // cas 拉取到offset
        if (offset < 0) return null; // 如果消费队列已空，则返回null
        return getReadCarriage(offset).getSegment(offset);
    }

    /**
     * 出队操作 - 读取数据（支持超时等待）
     * 查找当前位置，按顺序读取, 没有数据则等待
     *
     * @return 读取到的数据，如果队列为空或超时返回null
     */
    public byte[] dequeue() {
        while (true) {
            JSharedMemSegment segment = getSegment();
            if (segment == null) { // 如果队列已空，则返回null
                return null;
            } // 如果有数据，则尝试修改状态为正在读取
            if (segment.compareAndSetState(JSharedMemSegment.STATE_READABLE, JSharedMemSegment.STATE_READING)) {
                return segment.readContent();
            }//如果没有修改成功，说明被其他线程占用了
        }
    }

    public String getReaderPath() {
        return Dictionary.PARENT_DIR + "ipc_" + jSharedMemBaseInfo.getTopic() + ".reader";
    }

    @Override
    public void close() {
        try{
            System.out.println("【Reader】 执行销毁");
            this.accessFile.close();
            this.channel.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
