package com.compare.service.rocketmq;

import com.compare.model.TestConfig;
import com.compare.service.base.AbstractMQProducer;
import com.compare.model.MQMessage;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class RocketMQProducer extends AbstractMQProducer {
    private static final String MQ_TYPE = "RocketMQ";
    
    @Autowired
    private RocketMQTemplate rocketMQTemplate;
    
    private final String topicName;
    private boolean initialized = false;

    public RocketMQProducer(int topicIndex) {
        super(MQ_TYPE);
        this.topicName = TestConfig.getInstance().getTopicName(MQ_TYPE, topicIndex);
    }

    @Override
    public String getMQType() {
        return MQ_TYPE;
    }

    @Override
    public void initialize() {
        if (!initialized) {
//            rocketMQTemplate.start();
            initialized = true;
            System.out.println("RocketMQ生产者初始化完成，主题: " + topicName);
        }
    }

    @Override
    public void send(MQMessage message) throws Exception {
        long sendTime = System.currentTimeMillis();
        message.setSendTime(sendTime);
        
        Message<String> springMessage = MessageBuilder.withPayload(message.getContent())
            .setHeader("MESSAGE_ID", message.getMessageId())
            .build();

        SendResult result = rocketMQTemplate.syncSend(topicName, springMessage, 5000);
        
        if (result.getSendStatus() == SendStatus.SEND_OK) {
            recordSuccess(System.currentTimeMillis() - sendTime);
        } else {
            recordFail();
        }
    }

    @Override
    public CompletableFuture<Void> sendAsync(MQMessage message) {
        return CompletableFuture.runAsync(() -> {
            try {
                long sendTime = System.currentTimeMillis();
                message.setSendTime(sendTime);
                
                Message<String> springMessage = MessageBuilder.withPayload(message.getContent())
                    .setHeader("MESSAGE_ID", message.getMessageId())
                    .build();

                rocketMQTemplate.asyncSend(topicName, springMessage, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        recordSuccess(System.currentTimeMillis() - sendTime);
                    }

                    @Override
                    public void onException(Throwable e) {
                        recordFail();
                    }
                });
            } catch (Exception e) {
                recordFail();
            }
        });
    }

    @Override
    public void sendBatch(List<MQMessage> messages) throws Exception {
        long sendTime = System.currentTimeMillis();
        
        List<org.springframework.messaging.Message<String>> springMessages = new ArrayList<>();
        for (MQMessage message : messages) {
            message.setSendTime(sendTime);
            
            Message<String> springMessage = MessageBuilder.withPayload(message.getContent())
                .setHeader("MESSAGE_ID", message.getMessageId())
                .build();
            springMessages.add(springMessage);
        }

        SendResult result = rocketMQTemplate.syncSend(topicName, springMessages, 5000);
        
        if (result.getSendStatus() == SendStatus.SEND_OK) {
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
        rocketMQTemplate.destroy();
        System.out.println("RocketMQ生产者资源已清理");
    }
}
