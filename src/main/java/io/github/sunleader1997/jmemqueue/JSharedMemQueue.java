package io.github.sunleader1997.jmemqueue;

import io.github.sunleader1997.jmemqueue.enums.ContentSize;
import io.github.sunleader1997.jmemqueue.ttl.TimeToLive;

import java.util.concurrent.TimeUnit;

public class JSharedMemQueue {
    public static final TimeToLive DEF_TTL = new TimeToLive(7, TimeUnit.DAYS); // 默认保存 7天
    public static final int DEF_CAPACITY = 1024 * 1024;// 默认车厢承载 1024*1024 条数据（1GB）
    private final String topic;
    private final int msgMaxSize;
    private final int capacity;

    public JSharedMemQueue(String topic) {
        this.topic = topic;
        this.msgMaxSize = ContentSize.KB_1.getSize();
        this.capacity = DEF_CAPACITY;
    }

    public JSharedMemQueue(String topic, ContentSize contentSize, int capacity) {
        this.topic = topic;
        this.msgMaxSize = contentSize.getSize();
        this.capacity = capacity;
    }

    public JSharedMemQueue(String topic, int msgMaxSize, int capacity) {
        this.topic = topic;
        this.msgMaxSize = msgMaxSize;
        this.capacity = capacity;
    }

    public JSharedMemProducer createProducer() {
        JSharedMemBaseInfo jSharedMemBaseInfo = new JSharedMemBaseInfo(topic, msgMaxSize, capacity); // 基础信息
        return new JSharedMemProducer(jSharedMemBaseInfo);
    }

    /**
     * 创建临时reader,close时清理offset
     *
     * @return
     */
    public JSharedMemReader createReader() {
        JSharedMemBaseInfo jSharedMemBaseInfo = new JSharedMemBaseInfo(topic, msgMaxSize, capacity); // 基础信息
        return new JSharedMemReader(jSharedMemBaseInfo).needCleanFile();
    }

    /**
     * 创建 group 消费者，持久化 offset
     *
     * @param group
     * @return
     */
    public JSharedMemReader createReader(String group) {
        JSharedMemBaseInfo jSharedMemBaseInfo = new JSharedMemBaseInfo(topic, msgMaxSize, capacity); // 基础信息
        return new JSharedMemReader(jSharedMemBaseInfo, group);
    }

}
