package com.compare.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedisMessageProducerDemo {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "device_metrics";
    private static final String PRODUCER_GROUP = "producer_group";

    private static final String REDIS_KEY_PREFIX = "msg:content:";
    private static final long REDIS_TTL_MINUTES = 60;

    private final DefaultMQProducer producer;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public RedisMessageProducerDemo(StringRedisTemplate redisTemplate) {
        this.producer = new DefaultMQProducer(PRODUCER_GROUP);
        this.producer.setNamesrvAddr(NAMESRV_ADDR);
        this.producer.setVipChannelEnabled(false);
        this.redisTemplate = redisTemplate;
        this.objectMapper = new ObjectMapper();
    }

    public void start() throws Exception {
        producer.start();
        log.info("生产者已启动");
    }

    public void shutdown() {
        producer.shutdown();
        log.info("生产者已关闭");
    }

    public void sendDeviceMetrics(String deviceId, Map<String, Object> metrics) {
        try {
            String uniqueKey = UUID.randomUUID().toString();
            String redisKey = REDIS_KEY_PREFIX + uniqueKey;

            String jsonContent = objectMapper.writeValueAsString(metrics);

            redisTemplate.opsForValue().set(redisKey, jsonContent, REDIS_TTL_MINUTES, TimeUnit.MINUTES);

            Message message = new Message(TOPIC, deviceId, uniqueKey, uniqueKey.getBytes());

            SendResult result = producer.send(message);

            log.info("消息发送成功, deviceId={}, key={}, result={}", deviceId, uniqueKey, result.getSendStatus());

        } catch (Exception e) {
            log.error("消息发送失败, deviceId={}", deviceId, e);
        }
    }

    public static void main(String[] args) throws Exception {
        // 模拟Spring注入的RedisTemplate
        // StringRedisTemplate redisTemplate = new StringRedisTemplate();

        RedisMessageProducerDemo producerDemo = new RedisMessageProducerDemo(null);
        producerDemo.start();

        // 模拟发送百万设备数据
        for (int i = 0; i < 10; i++) {
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("temperature", 25.5 + i);
            metrics.put("humidity", 60 + i);
            metrics.put("pressure", 101.3 + i);
            metrics.put("timestamp", System.currentTimeMillis());
            metrics.put("data", new byte[1024]); // 模拟1KB数据

            producerDemo.sendDeviceMetrics("device_" + i, metrics);
        }

        Thread.sleep(2000);
        producerDemo.shutdown();
    }
}
