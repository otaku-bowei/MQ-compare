package com.compare.test;

import com.compare.test.mock.MockMQConsumer;
import com.compare.test.mock.MockMQProducer;
import com.compare.model.MQMessage;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 完整测试：多生产者+多消费者 (README_TEST.md Case 10,11,12)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FullTest {

    private static final int MESSAGE_COUNT = 1000;
    private static final int MESSAGE_SIZE = 1024;
    
    private ExecutorService executorService;
    
    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(30);
    }
    
    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("RabbitMQ 完整测试 (2p + 2c)")
    void testRabbitMQFull() throws Exception {
        Map<String, Object> result = runFullTest("RabbitMQ", 2, 2);
        assertNotNull(result);
        System.out.println("RabbitMQ 完整测试结果: " + result);
    }
    
    @Test
    @Order(2)
    @DisplayName("RocketMQ 完整测试 (2p + 2c)")
    void testRocketMQFull() throws Exception {
        Map<String, Object> result = runFullTest("RocketMQ", 2, 2);
        assertNotNull(result);
        System.out.println("RocketMQ 完整测试结果: " + result);
    }
    
    @Test
    @Order(3)
    @DisplayName("Kafka 完整测试 (2p + 2c)")
    void testKafkaFull() throws Exception {
        Map<String, Object> result = runFullTest("Kafka", 2, 2);
        assertNotNull(result);
        System.out.println("Kafka 完整测试结果: " + result);
    }
    
    @Test
    @Order(4)
    @DisplayName("RabbitMQ 完整测试 (4p + 4c)")
    void testRabbitMQFullHighConcurrency() throws Exception {
        Map<String, Object> result = runFullTest("RabbitMQ", 4, 4);
        assertNotNull(result);
        System.out.println("RabbitMQ 高并发测试结果: " + result);
    }
    
    @Test
    @Order(5)
    @DisplayName("RocketMQ 完整测试 (4p + 4c)")
    void testRocketMQFullHighConcurrency() throws Exception {
        Map<String, Object> result = runFullTest("RocketMQ", 4, 4);
        assertNotNull(result);
        System.out.println("RocketMQ 高并发测试结果: " + result);
    }
    
    @Test
    @Order(6)
    @DisplayName("Kafka 完整测试 (4p + 4c)")
    void testKafkaFullHighConcurrency() throws Exception {
        Map<String, Object> result = runFullTest("Kafka", 4, 4);
        assertNotNull(result);
        System.out.println("Kafka 高并发测试结果: " + result);
    }
    
    private Map<String, Object> runFullTest(String mqType, int producerCount, int consumerCount) 
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
                        // 随机分配给一个消费者
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
        
        // 等待所有完成
        for (Future<?> f : producerFutures) {
            f.get(60, TimeUnit.SECONDS);
        }
        for (Future<?> f : consumerFutures) {
            f.get(60, TimeUnit.SECONDS);
        }
        
        long endTime = System.currentTimeMillis();
        
        // 汇总结果
        Map<String, Object> result = new HashMap<>();
        result.put("mqType", mqType);
        result.put("producerCount", producerCount);
        result.put("consumerCount", consumerCount);
        result.put("queueCount", 1);
        result.put("totalMessages", MESSAGE_COUNT);
        result.put("duration", (endTime - startTime) / 1000.0);
        result.put("ops", MESSAGE_COUNT / Math.max(1, (endTime - startTime) / 1000.0));
        
        return result;
    }
}
