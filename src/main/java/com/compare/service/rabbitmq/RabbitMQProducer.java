package com.compare.service.rabbitmq;

import com.compare.model.TestConfig;
import com.compare.service.base.AbstractMQProducer;
import com.compare.config.RabbitMQConfig;
import com.compare.model.MQMessage;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public class RabbitMQProducer extends AbstractMQProducer {
    private static final String MQ_TYPE = "RabbitMQ";
    private Connection connection;
    private Channel channel;
    private final String queueName;

    public RabbitMQProducer(int queueIndex) {
        super(MQ_TYPE);
        this.queueName = TestConfig.getInstance().getQueueName(MQ_TYPE, queueIndex);
    }

    @Override
    public String getMQType() {
        return MQ_TYPE;
    }

    @Override
    public void initialize() {
        try {
            connection = RabbitMQConfig.getConnection();
            channel = connection.createChannel();
            
            channel.queueDeclare(queueName, true, false, false, null);
            
            channel.confirmSelect();
            
            System.out.println("RabbitMQ生产者初始化完成，队列: " + queueName);
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("RabbitMQ生产者初始化失败", e);
        }
    }

    @Override
    public void send(MQMessage message) throws Exception {
        long sendTime = System.currentTimeMillis();
        message.setSendTime(sendTime);
        
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
            .messageId(message.getMessageId())
            .timestamp(new java.util.Date())
            .deliveryMode(2)
            .build();

        channel.basicPublish("", queueName, properties, 
            message.getContent().getBytes());

        if (channel.waitForConfirms(5000)) {
            long latency = System.currentTimeMillis() - sendTime;
            recordSuccess(latency);
        } else {
            recordFail();
        }
    }

    @Override
    public CompletableFuture<Void> sendAsync(MQMessage message) {
        return CompletableFuture.runAsync(() -> {
            try {
                send(message);
            } catch (Exception e) {
                recordFail();
            }
        });
    }

    @Override
    public void sendBatch(java.util.List<MQMessage> messages) throws Exception {
        long sendTime = System.currentTimeMillis();
        
        for (MQMessage message : messages) {
            message.setSendTime(sendTime);
            
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .messageId(message.getMessageId())
                .timestamp(new java.util.Date())
                .deliveryMode(2)
                .build();

            channel.basicPublish("", queueName, properties, 
                message.getContent().getBytes());
        }

        if (channel.waitForConfirms(10000)) {
            for (int i = 0; i < messages.size(); i++) {
                recordSuccess(System.currentTimeMillis() - sendTime);
            }
        } else {
            for (int i = 0; i < messages.size(); i++) {
                recordFail();
            }
        }
    }

    @Override
    public void cleanup() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            RabbitMQConfig.closeConnection();
            System.out.println("RabbitMQ生产者资源已清理");
        } catch (IOException | TimeoutException e) {
            System.err.println("清理RabbitMQ生产者资源时出错: " + e.getMessage());
        }
    }
}
