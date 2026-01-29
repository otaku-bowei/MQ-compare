package com.compare.controller;

import com.compare.base.MQConsumer;
import com.compare.base.MQProducer;
import com.compare.kafka.KafkaConsumerImpl;
import com.compare.kafka.KafkaProducerImpl;
import com.compare.model.MQMessage;
import com.compare.model.TestConfig;
import com.compare.model.TestResult;
import com.compare.rabbitmq.RabbitMQConsumer;
import com.compare.rabbitmq.RabbitMQProducer;
import com.compare.rocketmq.RocketMQConsumer;
import com.compare.rocketmq.RocketMQProducer;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.*;

@RestController
@RequestMapping("/test")
public class MQTestController {

    private final TestConfig config = TestConfig.getInstance();
    private final Map<String, TestResult> results = new ConcurrentHashMap<>();
    private final Map<String, MQProducer> producers = new ConcurrentHashMap<>();
    private final Map<String, MQConsumer> consumers = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @PostMapping("/config")
    public Map<String, Object> configureTest(@RequestBody Map<String, Object> testConfig) {
        Map<String, Object> response = new HashMap<>();
        
        if (testConfig.containsKey("messageCount")) {
            config.setMessageCount((Integer) testConfig.get("messageCount"));
        }
        if (testConfig.containsKey("messageSize")) {
            config.setMessageSize((Integer) testConfig.get("messageSize"));
        }
        if (testConfig.containsKey("producerCount")) {
            config.setProducerCount((Integer) testConfig.get("producerCount"));
        }
        if (testConfig.containsKey("consumerCount")) {
            config.setConsumerCount((Integer) testConfig.get("consumerCount"));
        }
        
        response.put("status", "success");
        response.put("currentConfig", getCurrentConfig());
        
        return response;
    }

    @GetMapping("/config")
    public Map<String, Object> getCurrentConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("messageCount", config.getMessageCount());
        configMap.put("messageSize", config.getMessageSize());
        configMap.put("producerCount", config.getProducerCount());
        configMap.put("consumerCount", config.getConsumerCount());
        configMap.put("queueCount", config.getQueueCount());
        configMap.put("warmupIterations", config.getWarmupIterations());
        configMap.put("testIterations", config.getTestIterations());
        
        Map<String, Object> response = new HashMap<>();
        response.put("config", configMap);
        return response;
    }

    @PostMapping("/run/{mqType}")
    public Map<String, Object> runTest(@PathVariable String mqType) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            MQProducer producer = createProducer(mqType, 0);
            MQConsumer consumer = createConsumer(mqType, 0);
            
            producers.put(mqType, producer);
            consumers.put(mqType, consumer);
            
            TestResult result = runPerformanceTest(producer, consumer);
            results.put(mqType, result);
            
            response.put("status", "success");
            response.put("result", convertResultToMap(result));
            
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", e.getMessage());
            e.printStackTrace();
        }
        
        return response;
    }

    @PostMapping("/runAll")
    public Map<String, Object> runAllTests() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> testResults = new HashMap<>();
        
        String[] mqTypes = {"RabbitMQ", "RocketMQ", "Kafka"};
        
        for (String mqType : mqTypes) {
            try {
                MQProducer producer = createProducer(mqType, 0);
                MQConsumer consumer = createConsumer(mqType, 0);
                
                producers.put(mqType, producer);
                consumers.put(mqType, consumer);
                
                TestResult result = runPerformanceTest(producer, consumer);
                results.put(mqType, result);
                
                testResults.put(mqType, convertResultToMap(result));
                
            } catch (Exception e) {
                Map<String, String> errorMap = new HashMap<>();
                errorMap.put("status", "error");
                errorMap.put("message", e.getMessage());
                testResults.put(mqType, errorMap);
            }
        }
        
        response.put("status", "success");
        response.put("results", testResults);
        
        return response;
    }

    @GetMapping("/results")
    public Map<String, Object> getAllResults() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> allResults = new HashMap<>();
        
        for (Map.Entry<String, TestResult> entry : results.entrySet()) {
            allResults.put(entry.getKey(), convertResultToMap(entry.getValue()));
        }
        
        response.put("results", allResults);
        return response;
    }

    @GetMapping("/results/{mqType}")
    public Map<String, Object> getResult(@PathVariable String mqType) {
        Map<String, Object> response = new HashMap<>();
        TestResult result = results.get(mqType);
        
        if (result != null) {
            response.put("result", convertResultToMap(result));
        } else {
            response.put("message", "No result found for " + mqType);
        }
        
        return response;
    }

    @PostMapping("/cleanup")
    public Map<String, String> cleanup() {
        Map<String, String> response = new HashMap<>();
        
        for (MQProducer producer : producers.values()) {
            producer.cleanup();
        }
        for (MQConsumer consumer : consumers.values()) {
            consumer.cleanup();
        }
        
        producers.clear();
        consumers.clear();
        results.clear();
        
        response.put("status", "success");
        return response;
    }

    private MQProducer createProducer(String mqType, int index) {
        MQProducer producer;
        switch (mqType.toLowerCase()) {
            case "rabbitmq":
                producer = new RabbitMQProducer(index);
                break;
            case "rocketmq":
                producer = new RocketMQProducer(index);
                break;
            case "kafka":
                producer = new KafkaProducerImpl(index);
                break;
            default:
                throw new IllegalArgumentException("不支持的MQ类型: " + mqType);
        }
        producer.initialize();
        return producer;
    }

    private MQConsumer createConsumer(String mqType, int index) {
        MQConsumer consumer;
        switch (mqType.toLowerCase()) {
            case "rabbitmq":
                consumer = new RabbitMQConsumer(index);
                break;
            case "rocketmq":
                consumer = new RocketMQConsumer(index);
                break;
            case "kafka":
                consumer = new KafkaConsumerImpl(index);
                break;
            default:
                throw new IllegalArgumentException("不支持的MQ类型: " + mqType);
        }
        consumer.initialize();
        return consumer;
    }

    private TestResult runPerformanceTest(MQProducer producer, MQConsumer consumer) {
        producer.initialize();
        consumer.initialize();
        
        int warmupCount = config.getWarmupIterations();
        System.out.println("开始预热测试，消息数: " + warmupCount);
        
        try {
            for (int i = 0; i < warmupCount; i++) {
                MQMessage message = createTestMessage(i);
                producer.send(message);
                
                MQMessage received = consumer.receive();
                if (received != null) {
                    consumer.acknowledge(received.getMessageId());
                }
            }
        } catch (Exception e) {
            System.err.println("预热测试失败: " + e.getMessage());
        }
        
        producer.getTestResult().setStartTime(System.currentTimeMillis());
        System.out.println("开始正式测试，消息数: " + config.getMessageCount());
        
        try {
            CountDownLatch latch = new CountDownLatch(1);
            
            Future<?> consumerFuture = executorService.submit(() -> {
                try {
                    int received = 0;
                    while (received < config.getMessageCount()) {
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
            
            latch.countDown();
            
            for (int i = 0; i < config.getMessageCount(); i++) {
                MQMessage message = createTestMessage(i);
                producer.send(message);
                
                if (i % 1000 == 0) {
                    System.out.println("已发送 " + i + " 条消息");
                }
            }
            
            consumerFuture.get(60, TimeUnit.SECONDS);
            
        } catch (Exception e) {
            System.err.println("性能测试失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        producer.getTestResult().setEndTime(System.currentTimeMillis());
        
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        return producer.getTestResult();
    }

    private MQMessage createTestMessage(int index) {
        String messageId = UUID.randomUUID().toString();
        String content = "测试消息-" + index + "-" + generateTestContent();
        return new MQMessage(messageId, content);
    }

    private String generateTestContent() {
        StringBuilder sb = new StringBuilder();
        int size = config.getMessageSize();
        for (int i = 0; i < size; i++) {
            sb.append("X");
        }
        return sb.toString();
    }

    private Map<String, Object> convertResultToMap(TestResult result) {
        Map<String, Object> map = new HashMap<>();
        map.put("mqType", result.getMqType());
        map.put("totalMessages", result.getTotalMessages());
        map.put("successCount", result.getSuccessCount());
        map.put("failCount", result.getFailCount());
        map.put("ops", result.getOps());
        map.put("avgLatency", result.getAvgLatency());
        map.put("minLatency", result.getMinLatency());
        map.put("maxLatency", result.getMaxLatency());
        map.put("throughput", result.getThroughput());
        map.put("duration", (result.getEndTime() - result.getStartTime()) / 1000.0);
        return map;
    }
}
