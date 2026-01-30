package com.compare.service.rocketmq;

import com.compare.service.base.AbstractMQConsumer;
import com.compare.model.MQMessage;
import com.compare.model.TestConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Component
public class RocketMQConsumer extends AbstractMQConsumer {
    private static final String MQ_TYPE = "RocketMQ";
    
    @Autowired
    private org.apache.rocketmq.spring.core.RocketMQTemplate rocketMQTemplate;
    
    private DefaultMQPushConsumer consumer;
    private final String topicName;
    private CountDownLatch latch;
    private boolean initialized = false;

    public RocketMQConsumer(int topicIndex) {
        super();
        this.topicName = TestConfig.getInstance().getTopicName(MQ_TYPE, topicIndex);
    }

    @Override
    public String getMQType() {
        return MQ_TYPE;
    }

    @Override
    public void initialize() {
        if (!initialized) {
            try {
                consumer = new DefaultMQPushConsumer(
                    "rocketmq-consumer-group");
                consumer.setNamesrvAddr("127.0.0.1:9876");
                consumer.subscribe(topicName, "*");
                consumer.setConsumeMessageBatchMaxSize(1);
                
                latch = new CountDownLatch(1);
                
                consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                    for (MessageExt msg : msgs) {
                        String messageId = msg.getKeys();
                        String content = new String(msg.getBody());
                        
                        MQMessage message = createMessage(messageId, content);
                        message.setReceiveTime(System.currentTimeMillis());
                    }
                    
                    latch.countDown();
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                });
                
                consumer.start();
                initialized = true;
                running = true;
                System.out.println("RocketMQ消费者初始化完成，主题: " + topicName);
            } catch (MQClientException e) {
                throw new RuntimeException("RocketMQ消费者初始化失败", e);
            }
        }
    }

    @Override
    public MQMessage receive() throws Exception {
        if (latch != null && latch.getCount() > 0) {
            latch.await(1000, TimeUnit.MILLISECONDS);
        }
        
        return null;
    }

    @Override
    public List<MQMessage> receiveBatch(int batchSize) throws Exception {
        List<MQMessage> messages = new ArrayList<>();
        
        if (latch != null && latch.getCount() > 0) {
            latch.await(1000, TimeUnit.MILLISECONDS);
        }
        
        return messages;
    }

    @Override
    public void acknowledge(String messageId) {
    }

    @Override
    public void cleanup() {
        if (consumer != null) {
            consumer.shutdown();
        }
        running = false;
        System.out.println("RocketMQ消费者资源已清理");
    }
}
