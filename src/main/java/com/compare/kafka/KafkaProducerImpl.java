package com.compare.kafka;

import com.compare.base.AbstractMQProducer;
import com.compare.model.MQMessage;
import com.compare.model.TestConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.List;

public class KafkaProducerImpl extends AbstractMQProducer {
    private static final String MQ_TYPE = "Kafka";
    
    private Producer<String, String> producer;
    private final String topicName;
    private boolean initialized = false;

    public KafkaProducerImpl(int topicIndex) {
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
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            
            producer = new KafkaProducer<>(props);
            initialized = true;
            System.out.println("Kafka生产者初始化完成，主题: " + topicName);
        }
    }

    @Override
    public void send(MQMessage message) throws Exception {
        long sendTime = System.currentTimeMillis();
        message.setSendTime(sendTime);
        
        ProducerRecord<String, String> record = new ProducerRecord<>(
            topicName, 
            message.getMessageId(), 
            message.getContent()
        );
        
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
            
            recordSuccess(System.currentTimeMillis() - sendTime);
        } catch (ExecutionException e) {
            recordFail();
            throw new Exception("Kafka发送失败", e);
        }
    }

    @Override
    public CompletableFuture<Void> sendAsync(MQMessage message) {
        return CompletableFuture.runAsync(() -> {
            try {
                long sendTime = System.currentTimeMillis();
                message.setSendTime(sendTime);
                
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    topicName,
                    message.getMessageId(),
                    message.getContent()
                );
                
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        recordFail();
                    } else {
                        recordSuccess(System.currentTimeMillis() - sendTime);
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
        
        for (MQMessage message : messages) {
            message.setSendTime(sendTime);
            
            ProducerRecord<String, String> record = new ProducerRecord<>(
                topicName,
                message.getMessageId(),
                message.getContent()
            );
            
            producer.send(record);
        }
        
        producer.flush();
        
        for (int i = 0; i < messages.size(); i++) {
            recordSuccess(System.currentTimeMillis() - sendTime);
        }
    }

    @Override
    public void cleanup() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
        System.out.println("Kafka生产者资源已清理");
    }
}
