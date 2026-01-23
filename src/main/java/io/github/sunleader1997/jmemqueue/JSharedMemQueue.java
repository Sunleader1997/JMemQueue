package io.github.sunleader1997.jmemqueue;

import io.github.sunleader1997.jmemqueue.ttl.TimeToLive;

import java.util.concurrent.TimeUnit;

public class JSharedMemQueue {
    public static final TimeToLive DEF_TTL = new TimeToLive(7, TimeUnit.DAYS); // 默认保存 7天
    private final String topic;

    public JSharedMemQueue(String topic) {
        this.topic = topic;
    }

    public JSharedMemProducer createProducer() {
        return new JSharedMemProducer(this.topic);
    }
    public JSharedMemReader createReader() {
        return new JSharedMemReader(this.topic);
    }
    public JSharedMemReader createReader(String group){
        return new JSharedMemReader(this.topic,group);
    }

}
