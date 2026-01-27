import { useState, useEffect } from "react";
import { Play, Pause, RotateCcw, Zap, Database, Cpu } from "lucide-react";

type ThreadStatus = "idle" | "cas" | "writing" | "done";
type SmgStatus = "idle" | "mapping" | "writing" | "readable" | "reading";

interface SmgSlot {
  id: number;
  status: SmgStatus;
  offset?: number;
  producer?: string;
  consumer?: string;
  data?: string;
}

interface Thread {
  id: string;
  name: string;
  status: ThreadStatus;
  currentOffset?: number;
}

export default function Home() {
  const [isRunning, setIsRunning] = useState(false);
  const [smgSlots, setSmgSlots] = useState<SmgSlot[]>([]);
  const [producerThreads, setProducerThreads] = useState<Thread[]>([]);
  const [consumerThreads, setConsumerThreads] = useState<Thread[]>([]);
  const [currentOffset, setCurrentOffset] = useState(0);
  const [carriageCount, setCarriageCount] = useState(3);
  const [smgPerCarriage, setSmgPerCarriage] = useState(4);

  const initializeSlots = () => {
    const slots: SmgSlot[] = [];
    for (let i = 0; i < carriageCount * smgPerCarriage; i++) {
      slots.push({
        id: i,
        status: "idle",
      });
    }
    setSmgSlots(slots);
    setProducerThreads([
      { id: "A", name: "生产者 A", status: "idle" },
      { id: "B", name: "生产者 B", status: "idle" },
    ]);
    setConsumerThreads([
      { id: "C", name: "消费者 C", status: "idle" },
      { id: "D", name: "消费者 D", status: "idle" },
    ]);
    setCurrentOffset(0);
  };

  useEffect(() => {
    initializeSlots();
  }, [carriageCount, smgPerCarriage]);

  const getStatusColor = (status: SmgStatus) => {
    const colors = {
      idle: "bg-gray-800 border-gray-700",
      mapping: "bg-blue-900 border-blue-500",
      writing: "bg-yellow-900 border-yellow-500",
      readable: "bg-green-900 border-green-500",
      reading: "bg-purple-900 border-purple-500",
    };
    return colors[status];
  };

  const getThreadStatusColor = (status: ThreadStatus) => {
    const colors = {
      idle: "bg-gray-700 text-gray-300",
      cas: "bg-orange-600 text-white",
      writing: "bg-yellow-600 text-white",
      done: "bg-green-600 text-white",
    };
    return colors[status];
  };

  const animateProduction = async () => {
    const totalSlots = smgSlots.length;
    const messages = ["Hello", "World", "SMG", "Queue", "Fast", "Data"];

    const processOffset = async (offset: number, producerIndex: number, delay: number) => {
      const producer = producerThreads[producerIndex];

      await new Promise((resolve) => setTimeout(resolve, delay));

      setProducerThreads((prev) =>
        prev.map((p) =>
          p.id === producer.id ? { ...p, status: "cas" as ThreadStatus } : p
        )
      );

      await new Promise((resolve) => setTimeout(resolve, 600));

      setProducerThreads((prev) =>
        prev.map((p) =>
          p.id === producer.id
            ? { ...p, status: "writing" as ThreadStatus, currentOffset: offset }
            : p
        )
      );

      setSmgSlots((prev) =>
        prev.map((slot) =>
          slot.id === offset
            ? {
                ...slot,
                status: "mapping",
                offset: offset,
              }
            : slot
        )
      );

      await new Promise((resolve) => setTimeout(resolve, 600));

      const message = messages[offset % messages.length];
      setSmgSlots((prev) =>
        prev.map((slot) =>
          slot.id === offset
            ? {
                ...slot,
                status: "writing",
                data: message,
                producer: producer.id,
              }
            : slot
        )
      );

      await new Promise((resolve) => setTimeout(resolve, 600));

      setSmgSlots((prev) =>
        prev.map((slot) =>
          slot.id === offset ? { ...slot, status: "readable" } : slot
        )
      );

      setProducerThreads((prev) =>
        prev.map((p) =>
          p.id === producer.id ? { ...p, status: "done" as ThreadStatus } : p
        )
      );

      setCurrentOffset((prev) => Math.max(prev, offset + 1));

      await new Promise((resolve) => setTimeout(resolve, 300));

      setProducerThreads((prev) =>
        prev.map((p) =>
          p.id === producer.id ? { ...p, status: "idle" as ThreadStatus } : p
        )
      );
    };

    const promises: Promise<void>[] = [];
    for (let i = 0; i < totalSlots; i++) {
      const producerIndex = i % 2;
      const delay = Math.floor(i / 2) * 2400;
      promises.push(processOffset(i, producerIndex, delay));
    }

    await Promise.all(promises);
    await new Promise((resolve) => setTimeout(resolve, 1000));
    await animateConsumption();
  };

  const animateConsumption = async () => {
    const totalSlots = smgSlots.length;

    const processConsumption = async (offset: number, consumerIndex: number, delay: number) => {
      const consumer = consumerThreads[consumerIndex];

      await new Promise((resolve) => setTimeout(resolve, delay));

      setConsumerThreads((prev) =>
        prev.map((c) =>
          c.id === consumer.id ? { ...c, status: "cas" as ThreadStatus } : c
        )
      );

      await new Promise((resolve) => setTimeout(resolve, 500));

      setConsumerThreads((prev) =>
        prev.map((c) =>
          c.id === consumer.id
            ? { ...c, status: "writing" as ThreadStatus, currentOffset: offset }
            : c
        )
      );

      setSmgSlots((prev) =>
        prev.map((slot) =>
          slot.id === offset
            ? { ...slot, status: "reading", consumer: consumer.id }
            : slot
        )
      );

      await new Promise((resolve) => setTimeout(resolve, 500));

      setSmgSlots((prev) =>
        prev.map((slot) =>
          slot.id === offset ? { ...slot, status: "idle" } : slot
        )
      );

      setConsumerThreads((prev) =>
        prev.map((c) =>
          c.id === consumer.id ? { ...c, status: "done" as ThreadStatus } : c
        )
      );

      await new Promise((resolve) => setTimeout(resolve, 300));

      setConsumerThreads((prev) =>
        prev.map((c) =>
          c.id === consumer.id ? { ...c, status: "idle" as ThreadStatus } : c
        )
      );
    };

    const promises: Promise<void>[] = [];
    for (let i = 0; i < totalSlots; i++) {
      const consumerIndex = i % 2;
      const delay = Math.floor(i / 2) * 1800;
      promises.push(processConsumption(i, consumerIndex, delay));
    }

    await Promise.all(promises);
  };

  const handleStart = () => {
    if (!isRunning) {
      setIsRunning(true);
      animateProduction().then(() => {
        setIsRunning(false);
      });
    }
  };

  const handleReset = () => {
    setIsRunning(false);
    initializeSlots();
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 text-gray-100">
      <div className="container mx-auto px-4 py-8">
        <header className="mb-12 text-center">
          <div className="flex items-center justify-center gap-3 mb-4">
            <Database className="w-12 h-12 text-blue-400" />
            <h1 className="text-5xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
              JMemQueue
            </h1>
          </div>
          <p className="text-xl text-gray-400 mb-2">高性能跨进程共享内存队列系统</p>
          <div className="flex items-center justify-center gap-4 text-sm text-gray-500">
            <span className="flex items-center gap-1">
              <Zap className="w-4 h-4" />
              零拷贝机制
            </span>
            <span className="flex items-center gap-1">
              <Cpu className="w-4 h-4" />
              CAS无锁并发
            </span>
            <span className="flex items-center gap-1">
              <Database className="w-4 h-4" />
              高吞吐量
            </span>
          </div>
        </header>

        <section className="mb-16">
          <div className="bg-gray-800 rounded-lg p-8 border border-gray-700">
            <h2 className="text-3xl font-bold mb-6 text-center bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
              什么是 JMemQueue
            </h2>
            <p className="text-gray-300 text-lg leading-relaxed mb-8 text-center max-w-4xl mx-auto">
              JMemQueue 是一个基于 Java NIO 和共享内存技术构建的高性能跨进程通信（IPC）解决方案，利用内存映射文件（MappedByteBuffer）实现低延迟、高吞吐量的数据传输。
            </p>

            <div className="grid md:grid-cols-3 gap-6 mb-8">
              <div className="bg-gray-900 rounded-lg p-6 border border-gray-600">
                <div className="text-blue-400 mb-3">
                  <Database className="w-10 h-10" />
                </div>
                <h3 className="text-xl font-bold mb-2 text-white">适用场景</h3>
                <ul className="text-gray-400 text-sm space-y-2">
                  <li>• 单机服务器环境</li>
                  <li>• 多进程多服务高效通信</li>
                  <li>• 单机场景替代 Kafka</li>
                  <li>• 低延迟高吞吐量需求</li>
                </ul>
              </div>

              <div className="bg-gray-900 rounded-lg p-6 border border-gray-600">
                <div className="text-green-400 mb-3">
                  <Zap className="w-10 h-10" />
                </div>
                <h3 className="text-xl font-bold mb-2 text-white">核心特性</h3>
                <ul className="text-gray-400 text-sm space-y-2">
                  <li>• 零拷贝机制</li>
                  <li>• CAS无锁并发</li>
                  <li>• 高吞吐量</li>
                  <li>• 负载均衡</li>
                  <li>• 跨进程通信</li>
                  <li>• 持久化存储</li>
                </ul>
              </div>

              <div className="bg-gray-900 rounded-lg p-6 border border-gray-600">
                <div className="text-yellow-400 mb-3">
                  <Cpu className="w-10 h-10" />
                </div>
                <h3 className="text-xl font-bold mb-2 text-white">架构设计</h3>
                <div className="text-gray-400 text-sm">
                  <p className="mb-2 font-semibold text-gray-300">SMG 数据结构</p>
                  <ul className="space-y-1 text-xs font-mono">
                    <li className="flex justify-between">
                      <span>状态</span>
                      <span className="text-gray-500">0-3字节</span>
                    </li>
                    <li className="flex justify-between">
                      <span>数据大小</span>
                      <span className="text-gray-500">4-7字节</span>
                    </li>
                    <li className="flex justify-between">
                      <span>实际数据</span>
                      <span className="text-gray-500">8-1023字节</span>
                    </li>
                  </ul>
                </div>
              </div>
            </div>

            <div className="bg-gray-900 rounded-lg p-6 border border-gray-600">
              <h3 className="text-xl font-bold mb-4 text-center text-white">性能基准测试</h3>
              <div className="grid md:grid-cols-2 gap-4">
                <div className="bg-gray-800 rounded-lg p-4">
                  <h4 className="text-lg font-semibold mb-3 text-blue-400">Windows 环境</h4>
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between items-center">
                      <span className="text-gray-400">单线程吞吐量</span>
                      <span className="text-green-400 font-bold">200万+ msg/s</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-gray-400">4线程吞吐量</span>
                      <span className="text-green-400 font-bold">600万+ msg/s</span>
                    </div>
                  </div>
                </div>
                <div className="bg-gray-800 rounded-lg p-4">
                  <h4 className="text-lg font-semibold mb-3 text-purple-400">Linux 环境</h4>
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between items-center">
                      <span className="text-gray-400">4线程吞吐量</span>
                      <span className="text-green-400 font-bold">1800万+ msg/s</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-gray-400">平均延迟</span>
                      <span className="text-green-400 font-bold">&lt; 1 微秒</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section className="mb-12">
          <h2 className="text-3xl font-bold mb-6 text-center bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
            流程演示
          </h2>
        </section>

        <div className="grid lg:grid-cols-2 gap-8 mb-12">
          <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
            <h2 className="text-2xl font-bold mb-4 text-blue-400">SMG 车厢映射视图</h2>
            <p className="text-gray-400 mb-4 text-sm">
              每个SMG占用1024字节，包含状态、数据大小和实际数据。虚线表示动态映射状态。
            </p>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              {Array.from({ length: carriageCount }).map((_, carriageIdx) => (
                <div key={carriageIdx} className="bg-gray-900 rounded-lg p-3 border border-gray-600">
                  <div className="text-xs text-gray-500 mb-2 text-center">
                    车厢 {carriageIdx + 1}
                  </div>
                  <div className="space-y-2">
                    {Array.from({ length: smgPerCarriage }).map((_, smgIdx) => {
                      const slot = smgSlots[carriageIdx * smgPerCarriage + smgIdx];
                      return (
                        <div
                          key={smgIdx}
                          className={`relative border-2 rounded p-2 text-xs transition-all duration-300 ${
                            slot?.status === "mapping"
                              ? "border-dashed border-blue-400 animate-pulse"
                              : getStatusColor(slot?.status || "idle")
                          }`}
                        >
                          <div className="font-mono text-gray-400">
                            SMG-{slot?.id}
                          </div>
                          {slot?.offset !== undefined && (
                            <div className="text-gray-300 mt-1">Offset: {slot.offset}</div>
                          )}
                          {slot?.data && (
                            <div className="text-green-400 mt-1">"{slot.data}"</div>
                          )}
                          {slot?.producer && (
                            <div className="text-yellow-400 text-xs mt-1">
                              P-{slot.producer}
                            </div>
                          )}
                          {slot?.consumer && (
                            <div className="text-purple-400 text-xs mt-1">
                              C-{slot.consumer}
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                </div>
              ))}
            </div>
            <div className="mt-4 flex flex-wrap gap-2 text-xs">
              <span className="px-2 py-1 bg-gray-800 border border-gray-700 rounded">空闲</span>
              <span className="px-2 py-1 bg-blue-900 border border-blue-500 border-dashed rounded">映射中</span>
              <span className="px-2 py-1 bg-yellow-900 border border-yellow-500 rounded">写入中</span>
              <span className="px-2 py-1 bg-green-900 border border-green-500 rounded">可读</span>
              <span className="px-2 py-1 bg-purple-900 border border-purple-500 rounded">读取中</span>
            </div>
          </div>

          <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
            <h2 className="text-2xl font-bold mb-4 text-green-400">线程状态监控</h2>
            <div className="space-y-6">
              <div>
                <h3 className="text-lg font-semibold mb-3 text-yellow-400">生产者线程</h3>
                <div className="space-y-2">
                  {producerThreads.map((thread) => (
                    <div
                      key={thread.id}
                      className={`flex items-center justify-between p-3 rounded-lg transition-all duration-300 ${getThreadStatusColor(
                        thread.status
                      )}`}
                    >
                      <span className="font-semibold">{thread.name}</span>
                      <div className="flex items-center gap-2">
                        <span className="text-xs uppercase">{thread.status}</span>
                        {thread.currentOffset !== undefined && (
                          <span className="text-xs bg-gray-900 px-2 py-1 rounded">
                            Offset: {thread.currentOffset}
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div>
                <h3 className="text-lg font-semibold mb-3 text-purple-400">消费者线程</h3>
                <div className="space-y-2">
                  {consumerThreads.map((thread) => (
                    <div
                      key={thread.id}
                      className={`flex items-center justify-between p-3 rounded-lg transition-all duration-300 ${getThreadStatusColor(
                        thread.status
                      )}`}
                    >
                      <span className="font-semibold">{thread.name}</span>
                      <div className="flex items-center gap-2">
                        <span className="text-xs uppercase">{thread.status}</span>
                        {thread.currentOffset !== undefined && (
                          <span className="text-xs bg-gray-900 px-2 py-1 rounded">
                            Offset: {thread.currentOffset}
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="bg-gray-900 rounded-lg p-4 border border-gray-600">
                <div className="flex items-center justify-between">
                  <span className="text-gray-400">全局 Offset</span>
                  <span className="text-2xl font-mono text-blue-400">{currentOffset}</span>
                </div>
              </div>
            </div>

            <div className="mt-6 flex gap-3">
              <button
                onClick={handleStart}
                disabled={isRunning}
                className="flex-1 flex items-center justify-center gap-2 bg-green-600 hover:bg-green-700 disabled:bg-gray-600 text-white py-3 px-4 rounded-lg font-semibold transition-colors"
              >
                {isRunning ? (
                  <>
                    <Pause className="w-5 h-5" />
                    运行中...
                  </>
                ) : (
                  <>
                    <Play className="w-5 h-5" />
                    开始演示
                  </>
                )}
              </button>
              <button
                onClick={handleReset}
                disabled={isRunning}
                className="flex items-center justify-center gap-2 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 text-white py-3 px-4 rounded-lg font-semibold transition-colors"
              >
                <RotateCcw className="w-5 h-5" />
                重置
              </button>
            </div>
          </div>
        </div>

        <div className="bg-gray-800 rounded-lg p-6 border border-gray-700 mb-12">
          <h2 className="text-2xl font-bold mb-4 text-purple-400">工作流程说明</h2>
          <div className="grid md:grid-cols-2 gap-6">
            <div>
              <h3 className="text-lg font-semibold mb-3 text-yellow-400 flex items-center gap-2">
                <Zap className="w-5 h-5" />
                生产流程
              </h3>
              <ol className="space-y-2 text-gray-300 text-sm">
                <li className="flex gap-2">
                  <span className="bg-orange-600 text-white w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-xs">1</span>
                  <span>线程通过CAS操作竞争获取下一个可用的offset</span>
                </li>
                <li className="flex gap-2">
                  <span className="bg-orange-600 text-white w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-xs">2</span>
                  <span>成功获取offset的线程开始映射对应的SMG车厢（虚线表示映射状态）</span>
                </li>
                <li className="flex gap-2">
                  <span className="bg-orange-600 text-white w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-xs">3</span>
                  <span>线程将数据写入SMG，状态变为WRITING</span>
                </li>
                <li className="flex gap-2">
                  <span className="bg-orange-600 text-white w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-xs">4</span>
                  <span>写入完成后，状态变为READABLE，其他线程可以开始获取下一个offset</span>
                </li>
              </ol>
            </div>
            <div>
              <h3 className="text-lg font-semibold mb-3 text-purple-400 flex items-center gap-2">
                <Database className="w-5 h-5" />
                消费流程
              </h3>
              <ol className="space-y-2 text-gray-300 text-sm">
                <li className="flex gap-2">
                  <span className="bg-purple-600 text-white w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-xs">1</span>
                  <span>消费者线程通过CAS操作竞争获取下一个可消费的offset</span>
                </li>
                <li className="flex gap-2">
                  <span className="bg-purple-600 text-white w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-xs">2</span>
                  <span>成功获取offset的线程标记SMG状态为READING</span>
                </li>
                <li className="flex gap-2">
                  <span className="bg-purple-600 text-white w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-xs">3</span>
                  <span>线程读取数据并进行业务处理</span>
                </li>
                <li className="flex gap-2">
                  <span className="bg-purple-600 text-white w-5 h-5 rounded-full flex items-center justify-center flex-shrink-0 text-xs">4</span>
                  <span>读取完成后，SMG状态恢复为IDLE，可被重新使用</span>
                </li>
              </ol>
            </div>
          </div>
        </div>

        <QuickStartSection />
      </div>
    </div>
  );
}

function QuickStartSection() {
  const codeExample = `import io.github.sunleader1997.jmemqueue.JSharedMemQueue;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class JMemQueueExample {
    public static void main(String[] args) throws Exception {
        // 创建共享内存队列
        JSharedMemQueue queue = new JSharedMemQueue("my-topic");
        
        // ========== 生产者示例 ==========
        JSharedMemProducer producer = queue.createProducer();
        // 设置数据过期时间（可选）
        producer.setTimeToLive(10, TimeUnit.SECONDS);
        
        // 写入数据
        String message = "Hello, Shared Memory Queue!";
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        boolean success = producer.enqueue(data);
        System.out.println("消息生产成功: " + success);
        
        // ========== 消费者示例 ==========
        JSharedMemReader reader = queue.createReader();
        
        // 消费消息
        byte[] receivedData = reader.dequeue();
        if (receivedData != null) {
            String receivedMessage = new String(receivedData, StandardCharsets.UTF_8);
            System.out.println("收到消息: " + receivedMessage);
        }
        
        // ========== 批量生产示例 ==========
        final int MESSAGE_COUNT = 1000000;
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String msg = String.format("{\"index\":%d}", i);
            byte[] msgData = msg.getBytes(StandardCharsets.UTF_8);
            producer.enqueue(msgData);
        }
        System.out.println("批量生产完成，总计: " + producer.getTotalOffset());
        
        // ========== 多线程消费示例 ==========
        int threadCount = 4;
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try (JSharedMemReader reader = queue.createReader()) {
                    while (true) {
                        byte[] data = reader.dequeue();
                        if (data == null) break;
                        // 处理数据
                        String message = new String(data, StandardCharsets.UTF_8);
                        // 业务逻辑...
                    }
                }
            }).start();
        }
    }
}`;

  return (
    <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
      <h2 className="text-2xl font-bold mb-4 text-green-400 flex items-center gap-2">
        <Zap className="w-6 h-6" />
        Quick Start
      </h2>
      <div className="bg-gray-900 rounded-lg p-4 overflow-x-auto">
        <pre className="text-sm text-gray-300 font-mono whitespace-pre-wrap">
          {codeExample}
        </pre>
      </div>
      <div className="mt-4 grid md:grid-cols-2 gap-4 text-sm">
        <div className="bg-gray-900 rounded-lg p-4 border border-gray-700">
          <h3 className="font-semibold text-yellow-400 mb-2">依赖配置</h3>
          <pre className="text-gray-300 font-mono text-xs">
{`<dependency>
    <groupId>io.github.sunleader1997</groupId>
    <artifactId>JMemQueue</artifactId>
    <version>1.0.2</version>
</dependency>`}
          </pre>
        </div>
        <div className="bg-gray-900 rounded-lg p-4 border border-gray-700">
          <h3 className="font-semibold text-purple-400 mb-2">性能指标</h3>
          <ul className="space-y-1 text-gray-300">
            <li>Windows: 单线程 200万+ msg/s</li>
            <li>Windows: 4线程 600万+ msg/s</li>
            <li>Linux: 4线程 1800万+ msg/s</li>
            <li>延迟: 平均 &lt; 1 微秒</li>
          </ul>
        </div>
      </div>
    </div>
  );
}
