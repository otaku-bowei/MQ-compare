package com.compare.test;

import com.compare.test.mock.MockMQConsumer;
import com.compare.test.mock.MockMQProducer;
import com.compare.model.MQMessage;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 单生产者-单消费者测试 (README_TEST.md Case 1,2,3)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SingleProducerSingleConsumerTest {

    private static final int MESSAGE_COUNT = 100000;  // 测试用小数量
    private static final int MESSAGE_SIZE = 1024;
    
    private ExecutorService executorService;
    
    @BeforeEach
    void setUp() {
        executorService = Executors.newFixedThreadPool(10);
    }
    
    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }
    
    @Test
    @Order(1)
    @DisplayName("RabbitMQ 单生产者单消费者测试")
    void testRabbitMQSingleProducerSingleConsumer() throws Exception {
        MockMQProducer producer = new MockMQProducer("RabbitMQ", MESSAGE_SIZE);
        MockMQConsumer consumer = new MockMQConsumer("RabbitMQ", MESSAGE_SIZE);
        
        Map<String, Object> result = runTest(producer, consumer);
        
        assertNotNull(result);
        assertEquals("RabbitMQ", result.get("mqType"));
        assertTrue((Long) result.get("totalMessages") > 0);
        System.out.println("RabbitMQ 测试结果: " + result);
    }
    
    @Test
    @Order(2)
    @DisplayName("RocketMQ 单生产者单消费者测试")
    void testRocketMQSingleProducerSingleConsumer() throws Exception {
        MockMQProducer producer = new MockMQProducer("RocketMQ", MESSAGE_SIZE);
        MockMQConsumer consumer = new MockMQConsumer("RocketMQ", MESSAGE_SIZE);
        
        Map<String, Object> result = runTest(producer, consumer);
        
        assertNotNull(result);
        assertEquals("RocketMQ", result.get("mqType"));
        assertTrue((Long) result.get("totalMessages") > 0);
        System.out.println("RocketMQ 测试结果: " + result);
    }
    
    @Test
    @Order(3)
    @DisplayName("Kafka 单生产者单消费者测试")
    void testKafkaSingleProducerSingleConsumer() throws Exception {
        MockMQProducer producer = new MockMQProducer("Kafka", MESSAGE_SIZE);
        MockMQConsumer consumer = new MockMQConsumer("Kafka", MESSAGE_SIZE);
        
        Map<String, Object> result = runTest(producer, consumer);
        
        assertNotNull(result);
        assertEquals("Kafka", result.get("mqType"));
        assertTrue((Long) result.get("totalMessages") > 0);
        System.out.println("Kafka 测试结果: " + result);
    }
    
    private Map<String, Object> runTest(MockMQProducer producer, MockMQConsumer consumer) throws Exception {
        producer.startTest();
        
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
        
        // 发送消息
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            MQMessage message = new MQMessage(UUID.randomUUID().toString(), "test-" + i);
            consumer.simulateSend(message);
            producer.send(message);
        }
        
        consumerFuture.get(60, TimeUnit.SECONDS);
        producer.endTest();
        
        return producer.getResult();
    }
}
