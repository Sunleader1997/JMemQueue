package org.sunyaxing.imagine.jmemqueue;

import java.io.Closeable;
import java.io.IOException;

public class JSMProducer implements Closeable {

    private final JSharedMemQueue jSharedMemQueue;

    public JSMProducer(String topic, int capacity) throws Exception {
        this.jSharedMemQueue = new JSharedMemQueue(topic, capacity, true);
    }

    /**
     * 生产消息 失败则false
     */
    public boolean produce(String message, long timeoutMs) {
        long startTime = System.currentTimeMillis();
        while (true) {
            if (this.jSharedMemQueue.enqueue(message.getBytes())) {
                return true;
            }
            if (timeoutMs > 0 && System.currentTimeMillis() - startTime > timeoutMs) {
                return false;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void close() throws IOException {
        this.jSharedMemQueue.close();
    }

    public static void main(String[] args) {
        try (JSMProducer producer = new JSMProducer("test", 10)) {
            for (int i = 0; i < 8; i++) {
                producer.produce("hello world " + i, 10);
            }
        } catch (Exception e) {
        }
    }
}
