package com.compare.test;

import com.compare.test.mock.MockMQConsumer;
import com.compare.test.mock.MockMQProducer;
import com.compare.model.MQMessage;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 多消费者测试 (README_TEST.md Case 7,8,9)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MultiConsumerTest {

    private static final int MESSAGE_COUNT = 1000;
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
    @DisplayName("RabbitMQ 多消费者测试 (2 consumers)")
    void testRabbitMQMultiConsumers() throws Exception {
        int consumerCount = 2;
        MockMQProducer producer = new MockMQProducer("RabbitMQ", MESSAGE_SIZE);
        
        Map<String, Object> result = runMultiConsumerTest("RabbitMQ", producer, consumerCount);
        
        assertNotNull(result);
        assertEquals("RabbitMQ", result.get("mqType"));
        assertEquals(consumerCount, result.get("consumerCount"));
        System.out.println("RabbitMQ 多消费者测试结果: " + result);
    }
    
    @Test
    @Order(2)
    @DisplayName("RocketMQ 多消费者测试 (2 consumers)")
    void testRocketMQMultiConsumers() throws Exception {
        int consumerCount = 2;
        MockMQProducer producer = new MockMQProducer("RocketMQ", MESSAGE_SIZE);
        
        Map<String, Object> result = runMultiConsumerTest("RocketMQ", producer, consumerCount);
        
        assertNotNull(result);
        assertEquals("RocketMQ", result.get("mqType"));
        assertEquals(consumerCount, result.get("consumerCount"));
        System.out.println("RocketMQ 多消费者测试结果: " + result);
    }
    
    @Test
    @Order(3)
    @DisplayName("Kafka 多消费者测试 (2 consumers)")
    void testKafkaMultiConsumers() throws Exception {
        int consumerCount = 2;
        MockMQProducer producer = new MockMQProducer("Kafka", MESSAGE_SIZE);
        
        Map<String, Object> result = runMultiConsumerTest("Kafka", producer, consumerCount);
        
        assertNotNull(result);
        assertEquals("Kafka", result.get("mqType"));
        assertEquals(consumerCount, result.get("consumerCount"));
        System.out.println("Kafka 多消费者测试结果: " + result);
    }
    
    private Map<String, Object> runMultiConsumerTest(String mqType, 
                                                      MockMQProducer producer,
                                                      int consumerCount) throws Exception {
        List<MockMQConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < consumerCount; i++) {
            consumers.add(new MockMQConsumer(mqType, MESSAGE_SIZE));
        }
        
        int perConsumer = MESSAGE_COUNT / consumerCount;
        long startTime = System.currentTimeMillis();
        
        // 启动多个消费者
        List<Future<?>> futures = new ArrayList<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        
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
            futures.add(future);
        }
        
        startLatch.countDown();
        
        // 发送消息
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            MQMessage message = new MQMessage(UUID.randomUUID().toString(), "test-" + i);
            // 随机分配给一个消费者
            consumers.get(i % consumerCount).simulateSend(message);
            producer.send(message);
        }
        
        // 等待消费者完成
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        
        long endTime = System.currentTimeMillis();
        
        // 汇总结果
        Map<String, Object> result = new HashMap<>();
        result.put("mqType", mqType);
        result.put("producerCount", 1);
        result.put("consumerCount", consumerCount);
        result.put("queueCount", 1);
        result.put("totalMessages", MESSAGE_COUNT);
        result.put("duration", (endTime - startTime) / 1000.0);
        result.put("ops", MESSAGE_COUNT / ((endTime - startTime) / 1000.0));
        
        return result;
    }
}
