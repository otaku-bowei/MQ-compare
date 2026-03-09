package com.compare.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Map;

@Slf4j
public class RedisMessageConsumerDemo {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "device_metrics";
    private static final String CONSUMER_GROUP = "consumer_group";

    private static final String REDIS_KEY_PREFIX = "msg:content:";

    private final DefaultMQPushConsumer consumer;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public RedisMessageConsumerDemo(StringRedisTemplate redisTemplate) {
        this.consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        this.consumer.setNamesrvAddr(NAMESRV_ADDR);
        this.consumer.setConsumeThreadMin(10);
        this.consumer.setConsumeThreadMax(20);
        this.redisTemplate = redisTemplate;
        this.objectMapper = new ObjectMapper();
    }

    public void start() throws Exception {
        consumer.subscribe(TOPIC, "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {

                for (MessageExt msg : msgs) {
                    try {
                        String uniqueKey = new String(msg.getBody());

                        String redisKey = REDIS_KEY_PREFIX + uniqueKey;

                        String jsonContent = redisTemplate.opsForValue().get(redisKey);

                        if (jsonContent == null) {
                            log.warn("Redis中找不到消息内容, key={}", uniqueKey);
                            continue;
                        }

                        Map<String, Object> metrics = objectMapper.readValue(jsonContent, Map.class);

                        log.info("消费消息成功, deviceId={}, uniqueKey={}, metrics={}",
                                msg.getKeys(), uniqueKey, metrics);

                        redisTemplate.delete(redisKey);

                    } catch (Exception e) {
                        log.error("消息消费失败, msgId={}", msg.getMsgId(), e);
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        log.info("消费者已启动，从Redis读取消息内容");
    }

    public void shutdown() {
        consumer.shutdown();
        log.info("消费者已关闭");
    }

    public static void main(String[] args) throws Exception {
        // 模拟Spring注入的RedisTemplate
        // StringRedisTemplate redisTemplate = new StringRedisTemplate();

        RedisMessageConsumerDemo consumerDemo = new RedisMessageConsumerDemo(null);
        consumerDemo.start();

        Thread.sleep(60000);

        consumerDemo.shutdown();
    }
}
