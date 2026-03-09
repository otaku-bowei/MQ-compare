package com.compare.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.List;

public class RocketMQConsumerDemo {

    private static final String CONSUMER_GROUP = "consumer_group_test";
    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "test_topic_unique";

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);

        consumer.setNamesrvAddr(NAMESRV_ADDR);

        consumer.setConsumeThreadMin(5);
        consumer.setConsumeThreadMax(10);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        // ⭐ 关键配置：CLUSTERING模式 - 消息只会被一个消费者消费，不会重复
        // BROADCASTING模式 - 消息会被每个消费者都消费一次（会重复）
        consumer.setMessageModel(MessageModel.CLUSTERING);

        consumer.subscribe(TOPIC, "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.printf("收到消息: %s, 消息ID: %s, 队列: %d%n",
                        new String(msg.getBody()),
                        msg.getMsgId(),
                        msg.getQueueId());
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();

        System.out.println("消费者已启动，模式: CLUSTERING (抢占消费，不会重复)");

        Thread.sleep(60000);

        consumer.shutdown();
        System.out.println("消费者已关闭");
    }
}
