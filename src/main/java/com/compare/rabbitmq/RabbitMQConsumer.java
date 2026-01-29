package com.compare.rabbitmq;

import com.compare.base.AbstractMQConsumer;
import com.compare.config.RabbitMQConfig;
import com.compare.model.MQMessage;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class RabbitMQConsumer extends AbstractMQConsumer {
    private static final String MQ_TYPE = "RabbitMQ";
    private Connection connection;
    private Channel channel;
    private final String queueName;
    private Consumer consumer;

    public RabbitMQConsumer(int queueIndex) {
        super();
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
            
            channel.basicQos(1);
            
            consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                         AMQP.BasicProperties properties, 
                                         byte[] body) throws IOException {
                    String messageId = properties.getMessageId();
                    String content = new String(body);
                    
                    MQMessage message = createMessage(messageId, content);
                    message.setSendTime(System.currentTimeMillis());
                    message.setReceiveTime(System.currentTimeMillis());
                    
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            };
            
            channel.basicConsume(queueName, false, consumer);
            
            running = true;
            System.out.println("RabbitMQ消费者初始化完成，队列: " + queueName);
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException("RabbitMQ消费者初始化失败", e);
        }
    }

    @Override
    public MQMessage receive() throws Exception {
        GetResponse response = channel.basicGet(queueName, false);
        if (response != null) {
            AMQP.BasicProperties properties = response.getProps();
            String messageId = properties.getMessageId();
            String content = new String(response.getBody());
            
            MQMessage message = createMessage(messageId, content);
            message.setReceiveTime(System.currentTimeMillis());
            
            channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
            return message;
        }
        return null;
    }

    @Override
    public List<MQMessage> receiveBatch(int batchSize) throws Exception {
        List<MQMessage> messages = new ArrayList<>();
        
        for (int i = 0; i < batchSize; i++) {
            GetResponse response = channel.basicGet(queueName, false);
            if (response == null) {
                break;
            }
            
            AMQP.BasicProperties properties = response.getProps();
            String messageId = properties.getMessageId();
            String content = new String(response.getBody());
            
            MQMessage message = createMessage(messageId, content);
            message.setReceiveTime(System.currentTimeMillis());
            messages.add(message);
            
            channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
        }
        
        return messages;
    }

    @Override
    public void acknowledge(String messageId) {
    }

    @Override
    public void cleanup() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
            RabbitMQConfig.closeConnection();
            running = false;
            System.out.println("RabbitMQ消费者资源已清理");
        } catch (IOException | TimeoutException e) {
            System.err.println("清理RabbitMQ消费者资源时出错: " + e.getMessage());
        }
    }
}
