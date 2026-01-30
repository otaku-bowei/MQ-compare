package com.compare.service.base;

import com.compare.model.MQMessage;

import java.util.List;

public interface MQConsumer {
    String getMQType();
    
    void initialize();
    
    MQMessage receive() throws Exception;
    
    List<MQMessage> receiveBatch(int batchSize) throws Exception;
    
    void acknowledge(String messageId);
    
    void cleanup();
    
    int getReceivedCount();
    
    void reset();
}
