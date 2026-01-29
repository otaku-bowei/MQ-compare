package com.compare.kafka;

import com.compare.base.AbstractMQConsumer;
import com.compare.model.MQMessage;
import com.compare.model.TestConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerImpl extends AbstractMQConsumer {
    private static final String MQ_TYPE = "Kafka";
    
    private KafkaConsumer<String, String> consumer;
    private final String topicName;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private boolean initialized = false;

    public KafkaConsumerImpl(int topicIndex) {
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
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
            
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topicName));
            
            initialized = true;
            running.set(true);
            System.out.println("Kafka消费者初始化完成，主题: " + topicName);
        }
    }

    @Override
    public MQMessage receive() throws Exception {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        
        for (ConsumerRecord<String, String> record : records) {
            MQMessage message = createMessage(record.key(), record.value());
            message.setReceiveTime(System.currentTimeMillis());
            return message;
        }
        
        return null;
    }

    @Override
    public List<MQMessage> receiveBatch(int batchSize) throws Exception {
        List<MQMessage> messages = new ArrayList<>();
        
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        
        for (ConsumerRecord<String, String> record : records) {
            MQMessage message = createMessage(record.key(), record.value());
            message.setReceiveTime(System.currentTimeMillis());
            messages.add(message);
            
            if (messages.size() >= batchSize) {
                break;
            }
        }
        
        return messages;
    }

    @Override
    public void acknowledge(String messageId) {
    }

    @Override
    public void cleanup() {
        if (consumer != null) {
            consumer.close();
        }
        running.set(false);
        System.out.println("Kafka消费者资源已清理");
    }

    @Override
    public void reset() {
        super.reset();
        running.set(false);
    }
}
