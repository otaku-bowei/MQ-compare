package com.compare.test.mock;

import com.compare.model.MQMessage;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock消息生产者
 */
public class MockMQProducer {
    private final String mqType;
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failCount = new AtomicLong(0);
    private final AtomicLong totalMessages = new AtomicLong(0);
    private final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    private long startTime;
    private long endTime;
    private final int messageSize;
    
    public MockMQProducer(String mqType, int messageSize) {
        this.mqType = mqType;
        this.messageSize = messageSize;
    }
    
    public void send(MQMessage message) throws Exception {
        long sendTime = System.currentTimeMillis();
        message.setSendTime(sendTime);
        message.setContent(generateContent());
        
        // 模拟发送延迟 (1-10ms)
        Thread.sleep(ThreadLocalRandom.current().nextInt(1, 11));
        
        // 95%成功率
        if (ThreadLocalRandom.current().nextDouble() < 0.95) {
            successCount.incrementAndGet();
            latencies.add(System.currentTimeMillis() - sendTime);
        } else {
            failCount.incrementAndGet();
        }
        totalMessages.incrementAndGet();
    }
    
    public void sendAsync(MQMessage message) {
        CompletableFuture.runAsync(() -> {
            try {
                send(message);
            } catch (Exception e) {
                failCount.incrementAndGet();
                totalMessages.incrementAndGet();
            }
        });
    }
    
    public void startTest() {
        startTime = System.currentTimeMillis();
    }
    
    public void endTest() {
        endTime = System.currentTimeMillis();
    }
    
    public Map<String, Object> getResult() {
        Map<String, Object> result = new HashMap<>();
        result.put("mqType", mqType);
        result.put("totalMessages", totalMessages.get());
        result.put("successCount", successCount.get());
        result.put("failCount", failCount.get());
        result.put("duration", (endTime - startTime) / 1000.0);
        result.put("ops", calculateOps());
        result.put("avgLatency", calculateAvgLatency());
        result.put("throughput", calculateThroughput());
        return result;
    }
    
    private double calculateOps() {
        if (endTime - startTime == 0) return 0;
        return totalMessages.get() / ((endTime - startTime) / 1000.0);
    }
    
    private double calculateAvgLatency() {
        if (latencies.isEmpty()) return 0;
        return latencies.stream().mapToLong(Long::longValue).average().orElse(0);
    }
    
    private double calculateThroughput() {
        if (endTime - startTime == 0) return 0;
        return successCount.get() / ((endTime - startTime) / 1000.0);
    }
    
    private String generateContent() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < messageSize; i++) {
            sb.append("X");
        }
        return sb.toString();
    }
}
