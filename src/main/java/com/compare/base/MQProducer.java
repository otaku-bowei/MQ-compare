package com.compare.base;

import com.compare.model.MQMessage;
import com.compare.model.TestResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface MQProducer {
    String getMQType();
    
    void initialize();
    
    void send(MQMessage message) throws Exception;
    
    CompletableFuture<Void> sendAsync(MQMessage message);
    
    void sendBatch(List<MQMessage> messages) throws Exception;
    
    void cleanup();
    
    TestResult getTestResult();
}
