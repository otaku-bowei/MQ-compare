package com.compare.controller;

import com.compare.service.base.MQConsumer;
import com.compare.service.base.MQProducer;
import com.compare.service.kafka.KafkaConsumerImpl;
import com.compare.service.kafka.KafkaProducerImpl;
import com.compare.model.MQMessage;
import com.compare.model.TestConfig;
import com.compare.model.TestResult;
import com.compare.service.rabbitmq.RabbitMQConsumer;
import com.compare.service.rabbitmq.RabbitMQProducer;
import com.compare.service.rocketmq.RocketMQConsumer;
import com.compare.service.rocketmq.RocketMQProducer;
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
    private final Map<String, List<MQProducer>> producerClusters = new ConcurrentHashMap<>();
    private final Map<String, List<MQConsumer>> consumerClusters = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(20);

    // ==================== 配置接口 ====================

    @PostMapping("/config")
    public Map<String, Object> configureTest(@RequestBody Map<String, Object> testConfig) {
        Map<String, Object> response = new HashMap<>();
        
        if (testConfig.containsKey("messageCount")) {
            config.setMessageCount(((Number) testConfig.get("messageCount")).intValue());
        }
        if (testConfig.containsKey("messageSize")) {
            config.setMessageSize(((Number) testConfig.get("messageSize")).intValue());
        }
        if (testConfig.containsKey("producerCount")) {
            config.setProducerCount(((Number) testConfig.get("producerCount")).intValue());
        }
        if (testConfig.containsKey("consumerCount")) {
            config.setConsumerCount(((Number) testConfig.get("consumerCount")).intValue());
        }
        if (testConfig.containsKey("queueCount")) {
            config.setQueueCount(((Number) testConfig.get("queueCount")).intValue());
        }
        if (testConfig.containsKey("warmupIterations")) {
            config.setWarmupIterations(((Number) testConfig.get("warmupIterations")).intValue());
        }
        if (testConfig.containsKey("testIterations")) {
            config.setTestIterations(((Number) testConfig.get("testIterations")).intValue());
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

    // ==================== 基础测试接口（单生产者-单消费者）====================

    /**
     * 运行单组测试（1个生产者 + 1个消费者）
     * @param mqType RabbitMQ | RocketMQ | Kafka
     * @param queueIndex 队列索引（0开始）
     */
    @PostMapping("/run/{mqType}")
    public Map<String, Object> runTest(@PathVariable String mqType, 
                                       @RequestParam(defaultValue = "0") int queueIndex) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            cleanup(mqType);
            
            MQProducer producer = createProducer(mqType, queueIndex);
            MQConsumer consumer = createConsumer(mqType, queueIndex);
            
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

    // ==================== 多队列测试接口 ====================

    /**
     * 运行多队列测试（1个生产者 + 1个消费者）
     * @param mqType RabbitMQ | RocketMQ | Kafka
     * @param queueCount 队列数量
     */
    @PostMapping("/run/{mqType}/multi-queue")
    public Map<String, Object> runMultiQueueTest(@PathVariable String mqType,
                                                 @RequestParam(defaultValue = "4") int queueCount) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            config.setQueueCount(queueCount);
            cleanup(mqType);
            
            // 使用第一个队列进行测试
            MQProducer producer = createProducer(mqType, 0);
            MQConsumer consumer = createConsumer(mqType, 0);
            
            producers.put(mqType, producer);
            consumers.put(mqType, consumer);
            
            TestResult result = runPerformanceTest(producer, consumer);
            results.put(mqType, result);
            
            response.put("status", "success");
            response.put("queueCount", queueCount);
            response.put("result", convertResultToMap(result));
            
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", e.getMessage());
            e.printStackTrace();
        }
        
        return response;
    }

    // ==================== 多生产者测试接口 ====================

    /**
     * 运行多生产者测试
     * @param mqType RabbitMQ | RocketMQ | Kafka
     * @param producerCount 生产者数量
     * @param queueIndex 队列索引
     */
    @PostMapping("/run/{mqType}/multi-producer")
    public Map<String, Object> runMultiProducerTest(@PathVariable String mqType,
                                                    @RequestParam(defaultValue = "2") int producerCount,
                                                    @RequestParam(defaultValue = "0") int queueIndex) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            config.setProducerCount(producerCount);
            cleanup(mqType);
            
            List<MQProducer> producerList = new ArrayList<>();
            for (int i = 0; i < producerCount; i++) {
                MQProducer producer = createProducer(mqType, queueIndex);
                producerList.add(producer);
            }
            producerClusters.put(mqType, producerList);
            
            MQConsumer consumer = createConsumer(mqType, queueIndex);
            consumers.put(mqType, consumer);
            
            TestResult result = runMultiProducerTest(producerList, consumer);
            results.put(mqType, result);
            
            response.put("status", "success");
            response.put("producerCount", producerCount);
            response.put("result", convertResultToMap(result));
            
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", e.getMessage());
            e.printStackTrace();
        }
        
        return response;
    }

    // ==================== 多消费者测试接口 ====================

    /**
     * 运行多消费者测试
     * @param mqType RabbitMQ | RocketMQ | Kafka
     * @param consumerCount 消费者数量
     * @param queueIndex 队列索引
     */
    @PostMapping("/run/{mqType}/multi-consumer")
    public Map<String, Object> runMultiConsumerTest(@PathVariable String mqType,
                                                    @RequestParam(defaultValue = "2") int consumerCount,
                                                    @RequestParam(defaultValue = "0") int queueIndex) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            config.setConsumerCount(consumerCount);
            cleanup(mqType);
            
            MQProducer producer = createProducer(mqType, queueIndex);
            producers.put(mqType, producer);
            
            List<MQConsumer> consumerList = new ArrayList<>();
            for (int i = 0; i < consumerCount; i++) {
                MQConsumer consumer = createConsumer(mqType, queueIndex);
                consumerList.add(consumer);
            }
            consumerClusters.put(mqType, consumerList);
            
            TestResult result = runMultiConsumerTest(producer, consumerList);
            results.put(mqType, result);
            
            response.put("status", "success");
            response.put("consumerCount", consumerCount);
            response.put("result", convertResultToMap(result));
            
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", e.getMessage());
            e.printStackTrace();
        }
        
        return response;
    }

    // ==================== 完整测试用例接口 ====================

    /**
     * 运行完整的测试用例（支持多生产者+多消费者）
     * @param mqType RabbitMQ | RocketMQ | Kafka
     * @param queueCount 队列数
     * @param producerCount 生产者数
     * @param consumerCount 消费者数
     */
    @PostMapping("/run/{mqType}/full")
    public Map<String, Object> runFullTest(@PathVariable String mqType,
                                           @RequestParam(defaultValue = "1") Integer queueCount,
                                           @RequestParam(defaultValue = "1") Integer producerCount,
                                           @RequestParam(defaultValue = "1") Integer consumerCount) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            config.setQueueCount(queueCount);
            config.setProducerCount(producerCount);
            config.setConsumerCount(consumerCount);
            cleanup(mqType);
            
            List<MQProducer> producerList = new ArrayList<>();
            for (int i = 0; i < producerCount; i++) {
                MQProducer producer = createProducer(mqType, 0);
                producerList.add(producer);
            }
            producerClusters.put(mqType, producerList);
            
            List<MQConsumer> consumerList = new ArrayList<>();
            for (int i = 0; i < consumerCount; i++) {
                MQConsumer consumer = createConsumer(mqType, 0);
                consumerList.add(consumer);
            }
            consumerClusters.put(mqType, consumerList);
            
            TestResult result;
            if (producerCount > 1 && consumerCount > 1) {
                result = runFullTest(producerList, consumerList);
            } else if (producerCount > 1) {
                result = runMultiProducerTest(producerList, consumerList.get(0));
            } else if (consumerCount > 1) {
                result = runMultiConsumerTest(producerList.get(0), consumerList);
            } else {
                result = runPerformanceTest(producerList.get(0), consumerList.get(0));
            }
            
            results.put(mqType, result);
            
            response.put("status", "success");
            response.put("config", new HashMap<String, Integer>(){{
                put("queueCount", queueCount);
                put("producerCount", producerCount);
                put("consumerCount", consumerCount);
            }});
            response.put("result", convertResultToMap(result));
            
        } catch (Exception e) {
            response.put("status", "error");
            response.put("message", e.getMessage());
            e.printStackTrace();
        }
        
        return response;
    }

    // ==================== 批量测试接口 ====================

    /**
     * 运行所有测试用例（根据 README_TEST.md）
     */
    @PostMapping("/runAll")
    public Map<String, Object> runAllTests() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> testResults = new HashMap<>();
        
        String[] mqTypes = {"RabbitMQ", "RocketMQ", "Kafka"};
        int[][] testCases = {
            {1, 1, 1},   // 队列数, 生产者数, 消费者数
            {4, 1, 1},
            {4, 2, 1},
            {4, 1, 2},
            {4, 2, 2}
        };
        
        for (String mqType : mqTypes) {
            Map<String, Object> caseResults = new HashMap<>();
            
            for (int[] testCase : testCases) {
                int queueCount = testCase[0];
                int producerCount = testCase[1];
                int consumerCount = testCase[2];
                
                try {
                    cleanup(mqType);
                    
                    List<MQProducer> producerList = new ArrayList<>();
                    for (int i = 0; i < producerCount; i++) {
                        MQProducer producer = createProducer(mqType, 0);
                        producerList.add(producer);
                    }
                    producerClusters.put(mqType, producerList);
                    
                    List<MQConsumer> consumerList = new ArrayList<>();
                    for (int i = 0; i < consumerCount; i++) {
                        MQConsumer consumer = createConsumer(mqType, 0);
                        consumerList.add(consumer);
                    }
                    consumerClusters.put(mqType, consumerList);
                    
                    TestResult result;
                    String caseKey = String.format("queue%d_p%d_c%d", queueCount, producerCount, consumerCount);
                    
                    if (producerCount > 1 && consumerCount > 1) {
                        result = runFullTest(producerList, consumerList);
                    } else if (producerCount > 1) {
                        result = runMultiProducerTest(producerList, consumerList.get(0));
                    } else if (consumerCount > 1) {
                        result = runMultiConsumerTest(producerList.get(0), consumerList);
                    } else {
                        result = runPerformanceTest(producerList.get(0), consumerList.get(0));
                    }
                    
                    caseResults.put(caseKey, convertResultToMap(result));
                    
                } catch (Exception e) {
                    caseResults.put("error", e.getMessage());
                }
            }
            
            testResults.put(mqType, caseResults);
        }
        
        response.put("status", "success");
        response.put("results", testResults);
        
        return response;
    }

    // ==================== 结果查询接口 ====================

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

    // ==================== 清理接口 ====================

    @PostMapping("/cleanup")
    public Map<String, String> cleanup() {
        return cleanup(null);
    }

    private Map<String, String> cleanup(String mqType) {
        Map<String, String> response = new HashMap<>();
        
        if (mqType != null) {
            // 清理指定 MQ 类型
            if (producers.containsKey(mqType)) {
                producers.get(mqType).cleanup();
                producers.remove(mqType);
            }
            if (consumers.containsKey(mqType)) {
                consumers.get(mqType).cleanup();
                consumers.remove(mqType);
            }
            if (producerClusters.containsKey(mqType)) {
                for (MQProducer p : producerClusters.get(mqType)) {
                    p.cleanup();
                }
                producerClusters.remove(mqType);
            }
            if (consumerClusters.containsKey(mqType)) {
                for (MQConsumer c : consumerClusters.get(mqType)) {
                    c.cleanup();
                }
                consumerClusters.remove(mqType);
            }
            results.remove(mqType);
        } else {
            // 清理所有
            for (MQProducer producer : producers.values()) {
                producer.cleanup();
            }
            for (MQConsumer consumer : consumers.values()) {
                consumer.cleanup();
            }
            for (List<MQProducer> list : producerClusters.values()) {
                for (MQProducer p : list) p.cleanup();
            }
            for (List<MQConsumer> list : consumerClusters.values()) {
                for (MQConsumer c : list) c.cleanup();
            }
            producers.clear();
            consumers.clear();
            producerClusters.clear();
            consumerClusters.clear();
            results.clear();
        }
        
        response.put("status", "success");
        return response;
    }

    // ==================== 私有辅助方法 ====================

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

    /**
     * 单生产者-单消费者测试
     */
    private TestResult runPerformanceTest(MQProducer producer, MQConsumer consumer) {
        warmupTest(producer, consumer);
        
        int messageCount = config.getMessageCount();
        producer.getTestResult().setStartTime(System.currentTimeMillis());
        System.out.println("开始测试，消息数: " + messageCount);
        
        try {
            CountDownLatch latch = new CountDownLatch(1);
            
            Future<?> consumerFuture = executorService.submit(() -> {
                try {
                    int received = 0;
                    while (received < messageCount) {
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
            
            for (int i = 0; i < messageCount; i++) {
                producer.send(createTestMessage(i));
                if (i % 1000 == 0) {
                    System.out.println("已发送 " + i + " 条消息");
                }
            }
            
            consumerFuture.get(60, TimeUnit.SECONDS);
            
        } catch (Exception e) {
            System.err.println("测试失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        producer.getTestResult().setEndTime(System.currentTimeMillis());
        safeSleep(500);
        
        return producer.getTestResult();
    }

    /**
     * 多生产者测试
     */
    private TestResult runMultiProducerTest(List<MQProducer> producers, MQConsumer consumer) {
        warmupTest(producers.get(0), consumer);
        
        int messageCount = config.getMessageCount();
        int perProducer = messageCount / producers.size();
        
        producers.get(0).getTestResult().setStartTime(System.currentTimeMillis());
        System.out.println("开始多生产者测试，生产者数: " + producers.size() + "，每生产者消息: " + perProducer);
        
        try {
            CountDownLatch producerLatch = new CountDownLatch(producers.size());
            CountDownLatch startLatch = new CountDownLatch(1);
            
            // 启动消费者
            Future<?> consumerFuture = executorService.submit(() -> {
                try {
                    int received = 0;
                    while (received < messageCount) {
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
            for (int p = 0; p < producers.size(); p++) {
                final int producerIndex = p;
                Future<?> future = executorService.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < perProducer; i++) {
                            producers.get(producerIndex).send(createTestMessage(i));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        producerLatch.countDown();
                    }
                });
                futures.add(future);
            }
            
            startLatch.countDown();
            producerLatch.await();
            consumerFuture.get(60, TimeUnit.SECONDS);
            
        } catch (Exception e) {
            System.err.println("多生产者测试失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        producers.get(0).getTestResult().setEndTime(System.currentTimeMillis());
        safeSleep(500);
        
        return producers.get(0).getTestResult();
    }

    /**
     * 多消费者测试
     */
    private TestResult runMultiConsumerTest(MQProducer producer, List<MQConsumer> consumers) {
        warmupTest(producer, consumers.get(0));
        
        int messageCount = config.getMessageCount();
        int perConsumer = messageCount / consumers.size();
        
        producer.getTestResult().setStartTime(System.currentTimeMillis());
        System.out.println("开始多消费者测试，消费者数: " + consumers.size() + "，每消费者处理: " + perConsumer);
        
        try {
            CountDownLatch consumerLatch = new CountDownLatch(consumers.size());
            CountDownLatch startLatch = new CountDownLatch(1);
            
            // 启动多个消费者
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < consumers.size(); c++) {
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
                    } finally {
                        consumerLatch.countDown();
                    }
                });
                futures.add(future);
            }
            
            startLatch.countDown();
            
            // 发送消息
            for (int i = 0; i < messageCount; i++) {
                producer.send(createTestMessage(i));
                if (i % 1000 == 0) {
                    System.out.println("已发送 " + i + " 条消息");
                }
            }
            
            consumerLatch.await();
            
        } catch (Exception e) {
            System.err.println("多消费者测试失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        producer.getTestResult().setEndTime(System.currentTimeMillis());
        safeSleep(500);
        
        return producer.getTestResult();
    }

    /**
     * 完整测试（多生产者+多消费者）
     */
    private TestResult runFullTest(List<MQProducer> producers, List<MQConsumer> consumers) {
        warmupTest(producers.get(0), consumers.get(0));
        
        int messageCount = config.getMessageCount();
        int perProducer = messageCount / producers.size();
        int perConsumer = messageCount / consumers.size();
        
        producers.get(0).getTestResult().setStartTime(System.currentTimeMillis());
        System.out.println("开始完整测试，生产者: " + producers.size() + "，消费者: " + consumers.size());
        
        try {
            CountDownLatch startLatch = new CountDownLatch(1);
            
            // 启动消费者
            List<Future<?>> consumerFutures = new ArrayList<>();
            for (int c = 0; c < consumers.size(); c++) {
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
            for (int p = 0; p < producers.size(); p++) {
                final int producerIndex = p;
                Future<?> future = executorService.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < perProducer; i++) {
                            producers.get(producerIndex).send(createTestMessage(i));
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
            
        } catch (Exception e) {
            System.err.println("完整测试失败: " + e.getMessage());
            e.printStackTrace();
        }
        
        producers.get(0).getTestResult().setEndTime(System.currentTimeMillis());
        safeSleep(500);
        
        return producers.get(0).getTestResult();
    }

    private void warmupTest(MQProducer producer, MQConsumer consumer) {
        int warmupCount = config.getWarmupIterations();
        System.out.println("预热测试，消息数: " + warmupCount);
        
        for (int i = 0; i < warmupCount; i++) {
            try {
                producer.send(createTestMessage(i));
                MQMessage received = consumer.receive();
                if (received != null) {
                    consumer.acknowledge(received.getMessageId());
                }
            } catch (Exception e) {
                // 忽略预热错误
            }
        }
    }

    private void safeSleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
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
        if (result == null) {
            map.put("error", "No result");
            return map;
        }
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
