package com.compare.test.mock;

import com.compare.model.MQMessage;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock消息消费者
 */
public class MockMQConsumer {
    private final String mqType;
    private final BlockingQueue<MQMessage> messageQueue = new LinkedBlockingQueue<>();
    private final AtomicLong receivedCount = new AtomicLong(0);
    private final int messageSize;
    
    public MockMQConsumer(String mqType, int messageSize) {
        this.mqType = mqType;
        this.messageSize = messageSize;
    }
    
    /**
     * 模拟接收消息
     */
    public MQMessage receive() throws Exception {
        // 模拟接收延迟 (1-5ms)
        Thread.sleep(ThreadLocalRandom.current().nextInt(1, 6));
        
        MQMessage message = messageQueue.poll(100, TimeUnit.MILLISECONDS);
        if (message != null) {
            receivedCount.incrementAndGet();
            message.setReceiveTime(System.currentTimeMillis());
        }
        return message;
    }
    
    public List<MQMessage> receiveBatch(int batchSize) throws Exception {
        List<MQMessage> messages = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            MQMessage msg = receive();
            if (msg == null) break;
            messages.add(msg);
        }
        return messages;
    }
    
    /**
     * 模拟生产者发送消息到队列
     */
    public void simulateSend(MQMessage message) {
        message.setContent(generateContent());
        messageQueue.offer(message);
    }
    
    public void acknowledge(String messageId) {
        // Mock确认，始终成功
    }
    
    public long getReceivedCount() {
        return receivedCount.get();
    }
    
    private String generateContent() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < messageSize; i++) {
            sb.append("X");
        }
        return sb.toString();
    }
}
