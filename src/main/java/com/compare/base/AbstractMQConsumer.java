package com.compare.base;

import com.compare.model.MQMessage;

import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractMQConsumer implements MQConsumer {
    protected final AtomicLong receivedCount = new AtomicLong(0);
    protected volatile boolean running = false;

    @Override
    public int getReceivedCount() {
        return (int) receivedCount.get();
    }

    @Override
    public void reset() {
        receivedCount.set(0);
        running = false;
    }

    protected MQMessage createMessage(String messageId, String content) {
        MQMessage message = new MQMessage(messageId, content);
        message.setReceiveTime(System.currentTimeMillis());
        receivedCount.incrementAndGet();
        return message;
    }
}
