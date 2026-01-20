package io.github.sunleader1997.jmemqueue;

import io.github.sunleader1997.jmemqueue.exceptions.CarriageIndexMatchException;
import io.github.sunleader1997.jmemqueue.ttl.TimeToLive;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 车厢: 存储 SEGMENT 顺序集合
 */
public class JSharedMemCarriage implements AutoCloseable {

    private final JSharedMemBaseInfo jSharedMemBaseInfo;
    private final File carriageFile;
    private RandomAccessFile accessFile;
    private FileChannel channel;
    private ByteBuffer sharedMemory; // 整个共享内存，存储JSharedMemSegment
    private final TimeToLive timeToLive;
    // 当前车厢索引
    private final long currentCarriageIndex;

    private final long capacity;
    private boolean exist = true;

    public JSharedMemCarriage(JSharedMemBaseInfo jSharedMemBaseInfo, long offset, TimeToLive timeToLive) {
        this.jSharedMemBaseInfo = jSharedMemBaseInfo;
        this.capacity = jSharedMemBaseInfo.getCarriage();
        // 链接当前共享内存
        this.currentCarriageIndex = offset / capacity;
        String carriagePath = getCarriagePath(this.currentCarriageIndex);
        this.carriageFile = new File(carriagePath);
        this.timeToLive = timeToLive;
        System.out.println("【CARRIAGE】LOCATE AT [" + carriagePath + "] OFFSET BEGIN : " + offset);
    }

    /**
     * 内存映射，
     * read 模式下。如果文件不存在则 false
     *
     * @param mode
     * @return
     */
    public void mmap(FileChannel.MapMode mode) {
        try {
            this.accessFile = new RandomAccessFile(this.carriageFile, FileChannel.MapMode.READ_WRITE.equals(mode) ? "rw" : "r");
            this.channel = accessFile.getChannel();
            this.sharedMemory = channel.map(mode, 0, capacity * JSharedMemSegment.SMG_SIZE);
        } catch (IOException e) {
            this.exist = false;
        }
    }

    public boolean exist() {
        return this.exist;
    }

    public String getCarriagePath(long carriageIndex) {
        return Dictionary.PARENT_DIR + getFilePrefix() + "." + carriageIndex;
    }

    public String getFilePrefix() {
        return jSharedMemBaseInfo.getTopic() + ".dat";
    }

    private File[] listFiles(FileFilter fileFilter) {
        File parent = new File(Dictionary.PARENT_DIR);
        return parent.listFiles(pathname -> {
            boolean isDirectory = pathname.isDirectory();
            if (isDirectory) return false;
            boolean matched = pathname.getName().startsWith(getFilePrefix());
            if (matched) {
                return fileFilter.accept(pathname);
            }
            return false;
        });
    }

    private void cleanFile() {
        long cleanBefore = timeToLive.getCleanBefore();
        File[] files = listFiles(pathname -> {
            return pathname.lastModified() < cleanBefore;
        });
        for (File file : files) {
            System.out.printf("CLEAN DAT " + file.getName());
            file.delete();
        }
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
        try {
            System.out.println("【Carriage】 执行销毁");
            if (this.accessFile != null) {
                this.accessFile.close();
            }
            if (this.channel != null) {
                this.channel.close();
            }
            this.cleanFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
