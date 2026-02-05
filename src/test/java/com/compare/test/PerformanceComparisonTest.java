package com.compare.test;

import com.compare.test.mock.MockMQConsumer;
import com.compare.test.mock.MockMQProducer;
import com.compare.model.MQMessage;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 性能对比测试 - 对比三种MQ的性能
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PerformanceComparisonTest {

    private static final int MESSAGE_COUNT = 2000;
    private static final int MESSAGE_SIZE = 1024;
    
    private ExecutorService executorService;
    
    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(20);
    }
    
    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("对比测试：单生产者单消费者")
    void testSingleProducerSingleConsumerComparison() throws Exception {
        System.out.println("\n========== 单生产者单消费者对比测试 ==========");
        
        Map<String, Object> rabbitResult = runTest("RabbitMQ", 1, 1);
        Map<String, Object> rocketResult = runTest("RocketMQ", 1, 1);
        Map<String, Object> kafkaResult = runTest("Kafka", 1, 1);
        
        System.out.println("RabbitMQ: " + rabbitResult);
        System.out.println("RocketMQ: " + rocketResult);
        System.out.println("Kafka: " + kafkaResult);
        
        // 打印对比
        System.out.println("\n性能对比:");
        System.out.printf("RabbitMQ OPS: %.2f%n", rabbitResult.get("ops"));
        System.out.printf("RocketMQ OPS: %.2f%n", rocketResult.get("ops"));
        System.out.printf("Kafka OPS: %.2f%n", kafkaResult.get("ops"));
    }
    
    @Test
    @Order(2)
    @DisplayName("对比测试：多生产者多消费者")
    void testMultiProducerMultiConsumerComparison() throws Exception {
        System.out.println("\n========== 多生产者多消费者对比测试 (4p+4c) ==========");
        
        Map<String, Object> rabbitResult = runTest("RabbitMQ", 4, 4);
        Map<String, Object> rocketResult = runTest("RocketMQ", 4, 4);
        Map<String, Object> kafkaResult = runTest("Kafka", 4, 4);
        
        System.out.println("RabbitMQ: " + rabbitResult);
        System.out.println("RocketMQ: " + rocketResult);
        System.out.println("Kafka: " + kafkaResult);
    }
    
    @Test
    @Order(3)
    @DisplayName("对比测试：高吞吐量场景")
    void testHighThroughputComparison() throws Exception {
        System.out.println("\n========== 高吞吐量对比测试 (8p+8c) ==========");
        
        Map<String, Object> rabbitResult = runTest("RabbitMQ", 8, 8);
        Map<String, Object> rocketResult = runTest("RocketMQ", 8, 8);
        Map<String, Object> kafkaResult = runTest("Kafka", 8, 8);
        
        System.out.println("RabbitMQ: " + rabbitResult);
        System.out.println("RocketMQ: " + rocketResult);
        System.out.println("Kafka: " + kafkaResult);
    }
    
    private Map<String, Object> runTest(String mqType, int producerCount, int consumerCount) 
            throws Exception {
        List<MockMQProducer> producers = new ArrayList<>();
        for (int i = 0; i < producerCount; i++) {
            producers.add(new MockMQProducer(mqType, MESSAGE_SIZE));
        }
        
        List<MockMQConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < consumerCount; i++) {
            consumers.add(new MockMQConsumer(mqType, MESSAGE_SIZE));
        }
        
        int perProducer = MESSAGE_COUNT / producerCount;
        int perConsumer = MESSAGE_COUNT / consumerCount;
        long startTime = System.currentTimeMillis();
        
        CountDownLatch startLatch = new CountDownLatch(1);
        
        // 启动消费者
        List<Future<?>> consumerFutures = new ArrayList<>();
        for (int c = 0; c < consumerCount; c++) {
            final int consumerIndex = c;
            Future<?> future = executorService.submit(() -> {
                try {
                    startLatch.await();
                    int received = 0;
                    while (received < perConsumer) {
                        List<MQMessage> messages = consumers.get(consumerIndex).receiveBatch(100);
                        for (MQMessage msg : messages) {
                            consumers.get(consumerIndex).acknowledge(msg.getMessageId());
                            received++;
                        }
                        Thread.sleep(1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            consumerFutures.add(future);
        }
        
        // 启动生产者
        List<Future<?>> producerFutures = new ArrayList<>();
        for (int p = 0; p < producerCount; p++) {
            final int producerIndex = p;
            Future<?> future = executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < perProducer; i++) {
                        MQMessage message = new MQMessage(UUID.randomUUID().toString(), "test-" + i);
                        consumers.get((i + producerIndex) % consumerCount).simulateSend(message);
                        producers.get(producerIndex).send(message);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            producerFutures.add(future);
        }
        
        startLatch.countDown();
        
        for (Future<?> f : producerFutures) {
            f.get(60, TimeUnit.SECONDS);
        }
        for (Future<?> f : consumerFutures) {
            f.get(60, TimeUnit.SECONDS);
        }
        
        long endTime = System.currentTimeMillis();
        
        Map<String, Object> result = new HashMap<>();
        result.put("mqType", mqType);
        result.put("producerCount", producerCount);
        result.put("consumerCount", consumerCount);
        result.put("messageCount", MESSAGE_COUNT);
        result.put("duration", (endTime - startTime) / 1000.0);
        result.put("ops", MESSAGE_COUNT / ((endTime - startTime) / 1000.0));
        
        return result;
    }
}
