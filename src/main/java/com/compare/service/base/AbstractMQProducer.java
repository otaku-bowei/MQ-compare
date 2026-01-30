package com.compare.service.base;

import com.compare.common.LatencyRecorder;
import com.compare.model.TestConfig;
import com.compare.model.TestResult;

import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractMQProducer implements MQProducer {
    protected final TestConfig config = TestConfig.getInstance();
    protected final LatencyRecorder latencyRecorder = new LatencyRecorder();
    protected final TestResult result;
    protected final AtomicLong successCount = new AtomicLong(0);
    protected final AtomicLong failCount = new AtomicLong(0);
    protected final AtomicLong totalMessages = new AtomicLong(0);
    protected long startTime;
    protected long endTime;

    protected AbstractMQProducer(String mqType) {
        this.result = new TestResult(mqType);
    }

    @Override
    public TestResult getTestResult() {
        result.setTotalMessages(totalMessages.get());
        result.setSuccessCount(successCount.get());
        result.setFailCount(failCount.get());
        result.setStartTime(startTime);
        result.setEndTime(endTime);
        result.setAvgLatency(latencyRecorder.getAverage());
        result.setMinLatency(latencyRecorder.getMin());
        result.setMaxLatency(latencyRecorder.getMax());
        result.calculateMetrics();
        return result;
    }

    protected void recordSuccess(long latency) {
        successCount.incrementAndGet();
        totalMessages.incrementAndGet();
        latencyRecorder.record(latency);
    }

    protected void recordFail() {
        failCount.incrementAndGet();
        totalMessages.incrementAndGet();
    }

    protected String generateMessageContent() {
        StringBuilder sb = new StringBuilder();
        int size = config.getMessageSize();
        for (int i = 0; i < size; i++) {
            sb.append("X");
        }
        return sb.toString();
    }
}
