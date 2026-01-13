package org.sunyaxing.imagine.jmemqueue;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消费者测试用例 - 使用Reactor框架
 * 单线程scheduler负责dequeue消息，多线程负责打印（模拟业务消费数据）
 */
public class ConsumerTest {

    private static final int QUEUE_CAPACITY = 2048; // 队列大小 20MB
    private static final int MESSAGE_COUNT = QUEUE_CAPACITY * 10; // 消息总数
    private static final int BUSINESS_THREAD_COUNT = 1; // 业务处理线程数
    private static final String TOPIC = "topic1";

    /**
     * 模拟多线程生产消息
     */
    @Test
    public void produce() throws Exception {
        // 创建共享内存队列
        JSharedMemQueue queue = new JSharedMemQueue(TOPIC, QUEUE_CAPACITY, true);
        // 先生产一批消息
        System.out.println("开始生产消息...");
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = String.format("{\"index\":%d}", i);
            byte[] data = message.getBytes(StandardCharsets.UTF_8);
            queue.enqueue(data);
        }
        System.out.println("消息生产完成，总计: " + queue.getTotalOffset());
    }

    @Test
    public void createConsumer() throws Exception {
        JSharedMemQueue queue = new JSharedMemQueue(TOPIC, QUEUE_CAPACITY);
        AtomicInteger consumedCount = new AtomicInteger(0);
        // 创建多线程执行 dequeue
        CountDownLatch consumerLatch = new CountDownLatch(MESSAGE_COUNT);
        AtomicInteger nullCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(BUSINESS_THREAD_COUNT);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < BUSINESS_THREAD_COUNT; i++) {
            System.out.println("启动消费者线程: " + i);
            executor.execute(() -> {
                JSharedMemReader reader = queue.createReader();
                while (true) {
                    byte[] data = reader.dequeue();
                    if (data != null) {
                        String message = new String(data, StandardCharsets.UTF_8);
                        consumedCount.incrementAndGet();
                        consumerLatch.countDown();
                    }
                }
            });
        }
        boolean finished = consumerLatch.await(1, TimeUnit.MINUTES);
        executor.shutdown();
        long totalDuration = System.currentTimeMillis() - startTime;
        // 打印统计信息
        System.out.println("\n========== 消费者测试统计 ==========");
        System.out.println("队列容量: " + QUEUE_CAPACITY);
        System.out.println("生产消息总数: " + MESSAGE_COUNT);
        System.out.println("成功消费消息数: " + consumedCount.get());
        System.out.println("dequeue返回null次数: " + nullCount.get());
        System.out.println("总耗时: " + totalDuration + " ms");
        System.out.println("消费吞吐量: " + (consumedCount.get() * 1000L / totalDuration) + " msg/s");
        System.out.println("业务线程数: " + BUSINESS_THREAD_COUNT);
        System.out.println("===================================");
    }
}
