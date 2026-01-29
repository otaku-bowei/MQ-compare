package com.compare.model;

import java.util.concurrent.atomic.AtomicLong;

public class TestConfig {
    private static final TestConfig instance = new TestConfig();

    private int messageCount = 10000;
    private int messageSize = 1024;
    private int producerCount = 1;
    private int consumerCount = 1;
    private int queueCount = 1;
    private int warmupIterations = 1000;
    private int testIterations = 5;
    private String queuePrefix = "test-queue-";
    private String topicPrefix = "test-topic-";

    private TestConfig() {}

    public static TestConfig getInstance() {
        return instance;
    }

    public int getMessageCount() { return messageCount; }
    public void setMessageCount(int messageCount) { this.messageCount = messageCount; }

    public int getMessageSize() { return messageSize; }
    public void setMessageSize(int messageSize) { this.messageSize = messageSize; }

    public int getProducerCount() { return producerCount; }
    public void setProducerCount(int producerCount) { this.producerCount = producerCount; }

    public int getConsumerCount() { return consumerCount; }
    public void setConsumerCount(int consumerCount) { this.consumerCount = consumerCount; }

    public int getQueueCount() { return queueCount; }
    public void setQueueCount(int queueCount) { this.queueCount = queueCount; }

    public int getWarmupIterations() { return warmupIterations; }
    public void setWarmupIterations(int warmupIterations) { this.warmupIterations = warmupIterations; }

    public int getTestIterations() { return testIterations; }
    public void setTestIterations(int testIterations) { this.testIterations = testIterations; }

    public String getQueuePrefix() { return queuePrefix; }
    public void setQueuePrefix(String queuePrefix) { this.queuePrefix = queuePrefix; }

    public String getTopicPrefix() { return topicPrefix; }
    public void setTopicPrefix(String topicPrefix) { this.topicPrefix = topicPrefix; }

    public String getQueueName(String mqType, int index) {
        return queuePrefix + mqType.toLowerCase() + "-" + index;
    }

    public String getTopicName(String mqType, int index) {
        return topicPrefix + mqType.toLowerCase() + "-" + index;
    }
}
