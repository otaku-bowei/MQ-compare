package com.compare.demo;

import com.compare.model.MQMessage;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;

public class RocketMQProducerDemo {

    private static final String PRODUCER_GROUP = "producer_group_test";
    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "test_topic_unique";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDR);
        producer.setVipChannelEnabled(false);
        producer.start();

        System.out.println("生产者已启动");

        for (int i = 0; i < 10; i++) {
            Message msg = new Message(
                TOPIC,
                "TagA",
                "OrderID_" + i,
                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
            );

            msg.setKeys("unique_key_" + i);

            SendResult sendResult = producer.send(msg);
            System.out.printf("发送结果: %s, msgId: %s%n", sendResult.getSendStatus(), sendResult.getMsgId());
        }

        Thread.sleep(5000);
        producer.shutdown();
        System.out.println("生产者已关闭");
    }
}
