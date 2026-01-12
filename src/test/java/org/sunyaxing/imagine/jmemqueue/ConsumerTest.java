package org.sunyaxing.imagine.jmemqueue;

import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 消费者测试用例 - 使用Reactor框架
 * 单线程scheduler负责dequeue消息，多线程负责打印（模拟业务消费数据）
 */
public class ConsumerTest {

    private static final int QUEUE_CAPACITY = 2048; // 队列大小 1MB
    private static final int MESSAGE_COUNT = 204800; // 消息总数
    private static final int BUSINESS_THREAD_COUNT = 1; // 业务处理线程数
    private static final String TOPIC = "topic1";

    /**
     * 模拟多线程生产消息
     */
    @Test
    public void produce() throws Exception {
        // 创建共享内存队列
        JSharedMemQueue queue = new JSharedMemQueue(TOPIC, QUEUE_CAPACITY, true);
        queue.createWriteCarriage();
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
        JSharedMemReader reader = queue.getReader();
        AtomicInteger consumedCount = new AtomicInteger(0);
        // 创建多线程执行 dequeue
        CountDownLatch consumerLatch = new CountDownLatch(MESSAGE_COUNT);
        AtomicInteger nullCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(BUSINESS_THREAD_COUNT);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < BUSINESS_THREAD_COUNT; i++) {
            System.out.println("启动消费者线程: " + i);
            executor.execute(() -> {
                while (true) {
                    byte[] data = reader.dequeue();
                    if (data != null) {
                        String message = new String(data, StandardCharsets.UTF_8);
                        consumedCount.incrementAndGet();
                        consumerLatch.countDown();
                    } else {
                        System.out.println("FINISH" + reader.getReaderOffset());
                        return;
                    }
                }
            });
        }
        boolean finished = consumerLatch.await(10, TimeUnit.SECONDS);
        System.out.println("消费者结束"+reader.getReaderOffset());
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

    @Test
    public void createConsumerWithReactor() throws Exception {
        // 创建共享内存队列
        JSharedMemQueue queue = new JSharedMemQueue(TOPIC, QUEUE_CAPACITY);
        JSharedMemReader reader = queue.getReader();
        // 消费统计
        AtomicInteger consumedCount = new AtomicInteger(0);
        AtomicInteger nullCount = new AtomicInteger(0);
        CountDownLatch consumerLatch = new CountDownLatch(MESSAGE_COUNT);

        // 创建单线程scheduler用于dequeue操作
        Scheduler dequeueScheduler = Schedulers.newSingle("dequeue-thread");

        // 创建多线程scheduler用于业务处理（类似ChainDriver的processScheduler）
        Scheduler businessScheduler = Schedulers.newBoundedElastic(
                Schedulers.DEFAULT_BOUNDED_ELASTIC_SIZE,
                Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
                "business-thread");

        // 参考ChainDriver的实现方式：使用defer().repeat()实现主动拉取模式
        Flux<byte[]> dataDequeue = Flux.defer(() -> {
            byte[] data = reader.dequeue();
            if (data == null) {
                nullCount.incrementAndGet();
                // 取到null时等待1秒再继续
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    return Flux.empty();
                }
                return Flux.empty(); // 返回空流，不发射数据
            } else {
                return Flux.just(data); // 发射数据
            }
        }).repeat(); // 重复执行dequeue，类似ChainDriver

        // 构建消费者流
        Flux<Object> c = Flux.from(dataDequeue)
                .flatMap(data -> {
                    // 使用flatMap实现多线程并发处理，限制并发数为10（类似ChainDriver）
                    return Mono.fromRunnable(() -> {
                        String message = new String(data, StandardCharsets.UTF_8);
                        consumedCount.incrementAndGet();
                        consumerLatch.countDown();
                    }).subscribeOn(businessScheduler);
                }, 10) // 并发度为10，类似ChainDriver的flatMap配置
                .onBackpressureBuffer(100, (droppedData) -> {
                    // 背压缓冲策略，类似ChainDriver
                    System.err.println("背压告警: 缓冲区满，丢弃数据");
                })
                .onErrorContinue((throwable, o) -> {
                    System.err.println("消费过程出错: " + throwable.getMessage());
                    throwable.printStackTrace();
                })
                .subscribeOn(dequeueScheduler); // 在单线程上执行dequeue
        long startTime = System.currentTimeMillis();
        Disposable disposable = c.subscribe();
        // 等待所有消息被消费完成（最多等待5分钟）
        boolean finished = consumerLatch.await(20, TimeUnit.SECONDS);
        assertTrue(finished, "消费者未在预期时间内完成");

        long endTime = System.currentTimeMillis();
        long totalDuration = endTime - startTime;

        // 停止消费者流
        disposable.dispose();

        // 关闭scheduler
        dequeueScheduler.dispose();
        businessScheduler.dispose();

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

        // 验证所有消息都被消费
        assertEquals(MESSAGE_COUNT, consumedCount.get(), "应该消费所有消息");
    }

}
