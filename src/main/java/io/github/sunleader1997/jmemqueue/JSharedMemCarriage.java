package io.github.sunleader1997.jmemqueue;

import io.github.sunleader1997.jmemqueue.exceptions.CarriageIndexMatchException;
import io.github.sunleader1997.jmemqueue.ttl.JCleaner;
import io.github.sunleader1997.jmemqueue.ttl.TimeToLive;

import java.io.File;
import java.io.FileFilter;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * 车厢: 存储 SEGMENT 顺序集合
 */
public class JSharedMemCarriage implements AutoCloseable {
    public static final String CARRIAGE_FILE_ENDS = ".carriage";

    private final JSharedMemBaseInfo jSharedMemBaseInfo;
    private final File carriageFile;
    private RandomAccessFile accessFile;
    private FileChannel channel;
    private MappedByteBuffer baseConfigBuffer; // 基础配置信息
    private MappedByteBuffer sharedMemory; // 整个共享内存，存储JSharedMemSegment
    private final TimeToLive timeToLive;
    // 当前车厢索引
    private final long currentCarriageIndex;
    // 每个仓库的容量
    private final long capacity;
    private boolean exist = true;

    /**
     * 数据元大小开始位置
     */
    private static final int SGM_SIZE_OFFSET = 0;
    /**
     * segment 元数据开始位置
     */
    private static final int SEGMENT_OFFSET = 1024;

    public JSharedMemCarriage(JSharedMemBaseInfo jSharedMemBaseInfo, long offset, TimeToLive timeToLive, FileChannel.MapMode mode, int sgmSize) {
        this.jSharedMemBaseInfo = jSharedMemBaseInfo;
        this.capacity = jSharedMemBaseInfo.getCarriage();
        // 链接当前共享内存
        this.currentCarriageIndex = offset / capacity;
        Path carriagePath = getCarriagePath(this.currentCarriageIndex);
        this.carriageFile = carriagePath.toFile();
        this.timeToLive = timeToLive;
        System.out.println("【CARRIAGE】LOCATE AT [" + carriagePath + "] OFFSET BEGIN : " + offset);
        mmap(mode, sgmSize);
    }

    private void mmap(FileChannel.MapMode mode, int sgmSize) {
        try {
            // read 模式下，文件必须存在
            if (FileChannel.MapMode.READ_ONLY.equals(mode)) {
                if (this.carriageFile.exists()) {
                    this.accessFile = new RandomAccessFile(this.carriageFile, "r");
                    this.channel = accessFile.getChannel();
                    this.baseConfigBuffer = channel.map(mode, SGM_SIZE_OFFSET, SEGMENT_OFFSET);
                    this.sharedMemory = channel.map(mode, SEGMENT_OFFSET, capacity * sgmSize);
                } else {
                    this.exist = false;
                }
            } else { // write 模式，以下会自动创建
                this.accessFile = new RandomAccessFile(this.carriageFile, "rw");
                this.channel = accessFile.getChannel();
                this.baseConfigBuffer = channel.map(mode, 0, SEGMENT_OFFSET);
                AtomicVarHandle.setInt(this.baseConfigBuffer, SGM_SIZE_OFFSET, sgmSize);
                this.sharedMemory = channel.map(mode, SEGMENT_OFFSET, capacity * sgmSize);
            }
        } catch (Exception e) {
            this.exist = false;
        }
    }

    public int getSgmSize() {
        return AtomicVarHandle.getInt(this.baseConfigBuffer, SGM_SIZE_OFFSET);
    }


    public boolean exist() {
        return this.exist;
    }

    public Path getCarriagePath(long carriageIndex) {
        return Dictionary.getTopicDir(this.jSharedMemBaseInfo.getTopic()).resolve(getCarriageFileName(carriageIndex));
    }

    public String getCarriageFileName(long carriageIndex) {
        return carriageIndex + CARRIAGE_FILE_ENDS;
    }

    private File[] listFiles(FileFilter fileFilter) {
        Path parent = getCarriagePath(0).getParent();
        return parent.toFile().listFiles(pathname -> {
            boolean isDirectory = pathname.isDirectory();
            if (isDirectory) return false;
            boolean matched = pathname.getName().endsWith(CARRIAGE_FILE_ENDS);
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
        // 如果文件被消费者占用是无法成功删除的，所以每次都得遍历一遍
        for (File file : files) {
            boolean remove = file.delete();
            System.out.printf("CLEAN DAT " + file.getName() + " STATUS: " + remove);
        }
    }

    public JSharedMemSegment getSegment(long offset) {
        int compare = compareTo(offset);
        if (compare == 0) { // 直接取出数据块
            int index = (int) (offset % capacity);
            return new JSharedMemSegment(sharedMemory, getSgmSize(), index);
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

    public File getCarriageFile() {
        return this.carriageFile;
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
            if (this.baseConfigBuffer != null) {
                JCleaner.clean(this.baseConfigBuffer);
            }
            if (this.sharedMemory != null) {
                JCleaner.clean(this.sharedMemory);
            }
            this.cleanFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
