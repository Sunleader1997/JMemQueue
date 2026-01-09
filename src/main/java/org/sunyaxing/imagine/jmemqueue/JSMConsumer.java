package org.sunyaxing.imagine.jmemqueue;

import java.io.Closeable;
import java.io.IOException;

public class JSMConsumer implements Closeable {
    private final JSharedMemQueue jSharedMemQueue;

    public JSMConsumer(String topic, int capacity) throws Exception {
        this.jSharedMemQueue = new JSharedMemQueue(topic, capacity);
    }

    public String consume() {
        byte[] bytes = this.jSharedMemQueue.dequeue();
        return bytes == null ? null : new String(bytes);
    }

    @Override
    public void close() throws IOException {
        jSharedMemQueue.close();
    }

    public static void main(String[] args) {
        try (JSMConsumer consumer = new JSMConsumer("test", 10)) {
            while (true) {
                String message = consumer.consume();
                if (message == null) {
                    break;
                }
                System.out.println(message);
            }
        } catch (Exception e) {
        }
    }
}
