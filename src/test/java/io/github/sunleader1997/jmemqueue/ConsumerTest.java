package io.github.sunleader1997.jmemqueue;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 消费者测试用例 - 使用Reactor框架
 * 单线程scheduler负责dequeue消息，多线程负责打印（模拟业务消费数据）
 */
public class ConsumerTest {

    private static final int MESSAGE_COUNT = 1_000_000; // 总数据量
    private static final int BUSINESS_THREAD_COUNT = 1; // 业务处理线程数
    private static final String TOPIC = "topic1";

    /**
     * 模拟多线程生产消息
     */
    @Test
    public void produce() throws Exception {
        // 创建共享内存队列
        JSharedMemQueue queue = new JSharedMemQueue(TOPIC);
        try (JSharedMemProducer producer = queue.createProducer()) {
            producer.setTimeToLive(10, TimeUnit.SECONDS);
            // 先生产一批消息
            System.out.println("开始生产消息...");
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String message = String.format("{\"index\":%d}", i);
                byte[] data = message.getBytes(StandardCharsets.UTF_8);
                producer.enqueue(data);
            }
            System.out.println("消息生产完成，总计: " + producer.getTotalOffset());
        }
    }

    @Test
    public void createConsumer() throws Exception {
        JSharedMemQueue queue = new JSharedMemQueue(TOPIC);
        AtomicLong consumedCount = new AtomicLong(0);
        AtomicInteger nullCount = new AtomicInteger(0);
        CountDownLatch consumerLatch = new CountDownLatch(BUSINESS_THREAD_COUNT);
        ExecutorService executor = Executors.newFixedThreadPool(BUSINESS_THREAD_COUNT);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < BUSINESS_THREAD_COUNT; i++) {
            System.out.println("启动消费者线程: " + i);
            executor.execute(() -> {
                // 创建同一个GROUP的消费者
                try (JSharedMemReader reader = queue.createReader()) {
                    while (!Thread.interrupted()) {
                        byte[] data = reader.dequeue();
                        if (data != null) {
                            String message = new String(data, StandardCharsets.UTF_8);
                            consumedCount.incrementAndGet();
                        } else { // 消费结束
                            consumerLatch.countDown();
                            return;
                        }
                    }
                }
            });
        }
        boolean finished = consumerLatch.await(1, TimeUnit.MINUTES);
        executor.shutdown();
        long totalDuration = System.currentTimeMillis() - startTime;
        // 打印统计信息
        System.out.println("\n========== 消费者测试统计 ==========");
        //System.out.println("车厢容量: " + CARRIAGE_CAPACITY);
        //System.out.println("总消息容量: " + queue.getTotalOffset());
        System.out.println("成功消费消息数: " + consumedCount.get());
        System.out.println("dequeue返回null次数: " + nullCount.get());
        System.out.println("总耗时: " + totalDuration + " ms");
        System.out.println("消费吞吐量: " + (consumedCount.get() * 1000L / totalDuration) + " msg/s");
        System.out.println("业务线程数: " + BUSINESS_THREAD_COUNT);
        System.out.println("===================================");
    }

    /**
     * 通过自定义参数消费
     */
    @Test
    public void createByParam() {
        // 创建元数据4B,容纳100_000的车厢
        JSharedMemProducer producer = new JSharedMemProducer("topic2", 1000, 100_000, true);
        producer.setTimeToLive(10, TimeUnit.SECONDS);
        System.out.println("开始生产消息...");
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            byte[] data = ByteBuffer.allocate(4)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putInt(i)
                    .array();
            producer.enqueue(data);
        }
        System.out.println("消息生产完成，总计: " + producer.getTotalOffset());
    }

    @Test
    public void consumerByParam() {
        // 创建元数据4B,容纳100_000的车厢
        JSharedMemReader reader = new JSharedMemReader("topic2");
        AtomicLong consumedCount = new AtomicLong(0);
        long startTime = System.currentTimeMillis();
        while (true) {
            byte[] bytes = reader.dequeue();
            if (bytes == null) break;
            int result = ByteBuffer.wrap(bytes)
                    .order(ByteOrder.BIG_ENDIAN)
                    .getInt();
            if (result % 100_000 == 0) {
                System.out.println("Consume At :" + result);
            }
            consumedCount.incrementAndGet();
        }
        long totalDuration = System.currentTimeMillis() - startTime;
        // 打印统计信息
        System.out.println("\n========== 消费者测试统计 ==========");
        System.out.println("成功消费消息数: " + consumedCount.get());
        System.out.println("总耗时: " + totalDuration + " ms");
        System.out.println("消费吞吐量: " + (consumedCount.get() * 1000L / totalDuration) + " msg/s");
        System.out.println("业务线程数: " + BUSINESS_THREAD_COUNT);
        System.out.println("===================================");
    }
}
