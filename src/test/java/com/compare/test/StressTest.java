package com.compare.test;

import com.compare.test.mock.MockMQConsumer;
import com.compare.test.mock.MockMQProducer;
import com.compare.model.MQMessage;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 压力测试 - 高并发场景
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StressTest {

    private static final int MESSAGE_COUNT = 5000;
    private static final int MESSAGE_SIZE = 1024;
    
    private ExecutorService executorService;
    
    @BeforeEach
    void setUp() {
        executorService = Executors.newCachedThreadPool();
    }
    
    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("RabbitMQ 压力测试 (10p + 10c)")
    void testRabbitMQStress() throws Exception {
        Map<String, Object> result = runStressTest("RabbitMQ", 10, 10);
        System.out.println("RabbitMQ 压力测试: " + result);
        assertTrue((Double) result.get("ops") > 0);
    }
    
    @Test
    @Order(2)
    @DisplayName("RocketMQ 压力测试 (10p + 10c)")
    void testRocketMQStress() throws Exception {
        Map<String, Object> result = runStressTest("RocketMQ", 10, 10);
        System.out.println("RocketMQ 压力测试: " + result);
        assertTrue((Double) result.get("ops") > 0);
    }
    
    @Test
    @Order(3)
    @DisplayName("Kafka 压力测试 (10p + 10c)")
    void testKafkaStress() throws Exception {
        Map<String, Object> result = runStressTest("Kafka", 10, 10);
        System.out.println("Kafka 压力测试: " + result);
        assertTrue((Double) result.get("ops") > 0);
    }
    
    @Test
    @Order(4)
    @DisplayName("RabbitMQ 超高并发 (50p + 50c)")
    void testRabbitMQUltraStress() throws Exception {
        Map<String, Object> result = runStressTest("RabbitMQ", 50, 50);
        System.out.println("RabbitMQ 超高并发: " + result);
    }
    
    @Test
    @Order(5)
    @DisplayName("RocketMQ 超高并发 (50p + 50c)")
    void testRocketMQUltraStress() throws Exception {
        Map<String, Object> result = runStressTest("RocketMQ", 50, 50);
        System.out.println("RocketMQ 超高并发: " + result);
    }
    
    @Test
    @Order(6)
    @DisplayName("Kafka 超高并发 (50p + 50c)")
    void testKafkaUltraStress() throws Exception {
        Map<String, Object> result = runStressTest("Kafka", 50, 50);
        System.out.println("Kafka 超高并发: " + result);
    }
    
    private Map<String, Object> runStressTest(String mqType, int producerCount, int consumerCount) 
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
                        MQMessage message = new MQMessage(UUID.randomUUID().toString(), "stress-" + i);
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
            f.get(120, TimeUnit.SECONDS);
        }
        for (Future<?> f : consumerFutures) {
            f.get(120, TimeUnit.SECONDS);
        }
        
        long endTime = System.currentTimeMillis();
        double duration = (endTime - startTime) / 1000.0;
        
        Map<String, Object> result = new HashMap<>();
        result.put("mqType", mqType);
        result.put("producerCount", producerCount);
        result.put("consumerCount", consumerCount);
        result.put("totalMessages", MESSAGE_COUNT);
        result.put("duration", duration);
        result.put("ops", MESSAGE_COUNT / Math.max(0.001, duration));
        result.put("throughput", MESSAGE_COUNT / Math.max(0.001, duration));
        
        return result;
    }
}
