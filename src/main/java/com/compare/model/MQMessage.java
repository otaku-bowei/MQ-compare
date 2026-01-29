package com.compare.model;

public class MQMessage {
    private String messageId;
    private String content;
    private long timestamp;
    private long sendTime;
    private long receiveTime;

    public MQMessage() {}

    public MQMessage(String messageId, String content) {
        this.messageId = messageId;
        this.content = content;
        this.timestamp = System.currentTimeMillis();
    }

    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }

    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public long getSendTime() { return sendTime; }
    public void setSendTime(long sendTime) { this.sendTime = sendTime; }

    public long getReceiveTime() { return receiveTime; }
    public void setReceiveTime(long receiveTime) { this.receiveTime = receiveTime; }

    public long getLatency() {
        if (receiveTime > sendTime) {
            return receiveTime - sendTime;
        }
        return 0;
    }
}
