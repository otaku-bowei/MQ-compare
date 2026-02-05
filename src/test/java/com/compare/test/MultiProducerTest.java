package com.compare.test;

import com.compare.test.mock.MockMQConsumer;
import com.compare.test.mock.MockMQProducer;
import com.compare.model.MQMessage;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 多生产者测试 (README_TEST.md Case 4,5,6)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MultiProducerTest {

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
    @DisplayName("RabbitMQ 多生产者测试 (2 producers)")
    void testRabbitMQMultiProducers() throws Exception {
        int producerCount = 2;
        MockMQConsumer consumer = new MockMQConsumer("RabbitMQ", MESSAGE_SIZE);
        
        Map<String, Object> result = runMultiProducerTest("RabbitMQ", producerCount, consumer);
        
        assertNotNull(result);
        assertEquals("RabbitMQ", result.get("mqType"));
        assertEquals(producerCount, result.get("producerCount"));
        System.out.println("RabbitMQ 多生产者测试结果: " + result);
    }
    
    @Test
    @Order(2)
    @DisplayName("RocketMQ 多生产者测试 (2 producers)")
    void testRocketMQMultiProducers() throws Exception {
        int producerCount = 2;
        MockMQConsumer consumer = new MockMQConsumer("RocketMQ", MESSAGE_SIZE);
        
        Map<String, Object> result = runMultiProducerTest("RocketMQ", producerCount, consumer);
        
        assertNotNull(result);
        assertEquals("RocketMQ", result.get("mqType"));
        assertEquals(producerCount, result.get("producerCount"));
        System.out.println("RocketMQ 多生产者测试结果: " + result);
    }
    
    @Test
    @Order(3)
    @DisplayName("Kafka 多生产者测试 (2 producers)")
    void testKafkaMultiProducers() throws Exception {
        int producerCount = 2;
        MockMQConsumer consumer = new MockMQConsumer("Kafka", MESSAGE_SIZE);
        
        Map<String, Object> result = runMultiProducerTest("Kafka", producerCount, consumer);
        
        assertNotNull(result);
        assertEquals("Kafka", result.get("mqType"));
        assertEquals(producerCount, result.get("producerCount"));
        System.out.println("Kafka 多生产者测试结果: " + result);
    }
    
    private Map<String, Object> runMultiProducerTest(String mqType, int producerCount, 
                                                      MockMQConsumer consumer) throws Exception {
        List<MockMQProducer> producers = new ArrayList<>();
        for (int i = 0; i < producerCount; i++) {
            producers.add(new MockMQProducer(mqType, MESSAGE_SIZE));
        }
        
        int perProducer = MESSAGE_COUNT / producerCount;
        long startTime = System.currentTimeMillis();
        
        // 启动消费者
        Future<?> consumerFuture = executorService.submit(() -> {
            try {
                int received = 0;
                while (received < MESSAGE_COUNT) {
                    List<MQMessage> messages = consumer.receiveBatch(100);
                    for (MQMessage msg : messages) {
                        consumer.acknowledge(msg.getMessageId());
                        received++;
                    }
                    Thread.sleep(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        // 启动多个生产者
        List<Future<?>> futures = new ArrayList<>();
        CountDownLatch startLatch = new CountDownLatch(1);
        
        for (int p = 0; p < producerCount; p++) {
            final int producerIndex = p;
            Future<?> future = executorService.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < perProducer; i++) {
                        MQMessage message = new MQMessage(UUID.randomUUID().toString(), "test-" + i);
                        consumer.simulateSend(message);
                        producers.get(producerIndex).send(message);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            futures.add(future);
        }
        
        startLatch.countDown();
        
        // 等待完成
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        consumerFuture.get(60, TimeUnit.SECONDS);
        
        long endTime = System.currentTimeMillis();
        
        // 汇总结果
        Map<String, Object> result = new HashMap<>();
        result.put("mqType", mqType);
        result.put("producerCount", producerCount);
        result.put("consumerCount", 1);
        result.put("queueCount", 1);
        result.put("totalMessages", MESSAGE_COUNT);
        result.put("duration", (endTime - startTime) / 1000.0);
        result.put("ops", MESSAGE_COUNT / ((endTime - startTime) / 1000.0));
        
        return result;
    }
}
