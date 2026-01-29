package com.compare.model;

public class TestResult {
    private String mqType;
    private long totalMessages;
    private long successCount;
    private long failCount;
    private long startTime;
    private long endTime;
    private double ops;
    private double avgLatency;
    private long minLatency;
    private long maxLatency;
    private double throughput;

    public TestResult() {}

    public TestResult(String mqType) {
        this.mqType = mqType;
    }

    public String getMqType() { return mqType; }
    public void setMqType(String mqType) { this.mqType = mqType; }

    public long getTotalMessages() { return totalMessages; }
    public void setTotalMessages(long totalMessages) { this.totalMessages = totalMessages; }

    public long getSuccessCount() { return successCount; }
    public void setSuccessCount(long successCount) { this.successCount = successCount; }

    public long getFailCount() { return failCount; }
    public void setFailCount(long failCount) { this.failCount = failCount; }

    public long getStartTime() { return startTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }

    public long getEndTime() { return endTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }

    public double getOps() { return ops; }
    public void setOps(double ops) { this.ops = ops; }

    public double getAvgLatency() { return avgLatency; }
    public void setAvgLatency(double avgLatency) { this.avgLatency = avgLatency; }

    public long getMinLatency() { return minLatency; }
    public void setMinLatency(long minLatency) { this.minLatency = minLatency; }

    public long getMaxLatency() { return maxLatency; }
    public void setMaxLatency(long maxLatency) { this.maxLatency = maxLatency; }

    public double getThroughput() { return throughput; }
    public void setThroughput(double throughput) { this.throughput = throughput; }

    public void calculateMetrics() {
        if (endTime > startTime) {
            double durationSeconds = (endTime - startTime) / 1000.0;
            ops = successCount / durationSeconds;
            throughput = successCount / durationSeconds / 1024 / 1024;
        }
    }

    @Override
    public String toString() {
        return String.format(
            "MQ类型: %s, 总消息数: %d, 成功: %d, 失败: %d, OPS: %.2f, 平均延迟: %.2fms, 最大延迟: %dms, 吞吐量: %.2f MB/s",
            mqType, totalMessages, successCount, failCount, ops, avgLatency, maxLatency, throughput
        );
    }
}
