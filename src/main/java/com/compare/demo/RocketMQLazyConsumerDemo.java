package com.compare.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class RocketMQLazyConsumerDemo {

    private static final String CONSUMER_GROUP = "consumer_group_test";
    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "test_topic";

    private DefaultMQPushConsumer consumer;

    public void start() {
        consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDR);
        consumer.setConsumeThreadMin(2);
        consumer.setConsumeThreadMax(4);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setMessageModel(org.apache.rocketmq.client.consumer.MessageModel.CLUSTERING);
        consumer.setMessageQueueListener(new org.apache.rocketmq.client.consumer.listener.MessageQueueListener() {
        });

        try {
            consumer.subscribe(TOPIC, "*");
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        log.info("收到消息: {}", new String(msg.getBody()));
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();
            log.info("RocketMQ消费者启动成功");

        } catch (RemotingException | org.apache.rocketmq.client.exception.MQClientException e) {
            log.warn("RocketMQ连接失败，跳过启动: {}", e.getMessage());
            consumer = null;
        }
    }

    public void shutdown() {
        if (consumer != null) {
            try {
                consumer.shutdown();
                log.info("RocketMQ消费者已关闭");
            } catch (Exception e) {
                log.error("关闭消费者失败", e);
            }
        }
    }

    public boolean isRunning() {
        return consumer != null;
    }

    public static void main(String[] args) {
        RocketMQLazyConsumerDemo demo = new RocketMQLazyConsumerDemo();

        Runtime.getRuntime().addShutdownHook(new Thread(demo::shutdown));

        demo.start();

        if (demo.isRunning()) {
            log.info("消费者正在运行...");
        } else {
            log.warn("消费者启动失败，但应用继续运行...");
        }

        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
