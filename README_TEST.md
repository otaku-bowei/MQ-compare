# 一、 统一测试架构
```
   text
   ┌─────────────────────────────────────────┐
   │           测试控制中心                    │
   │  • 参数配置管理                           │
   │  • 测试用例调度                           │
   │  • 结果收集分析                           │
   └─────────────────┬───────────────────────┘
                     │
   ┌─────────────────┼──────────────────────┐
   │                 │                      │
   ┌───▼────┐   ┌────▼────┐            ┌────▼────┐
   │生产者集群│   │队列集群  │            │消费者集群│
   │ (压测机)│   │(被测系统)│            │ (压测机)│
   └─────────┘  └─────────┘            └─────────┘

```

# 二、 测试用例
|           | 队列数 | 生产者个数 | 消费者个数 | 结果(ops) |  
|-----------|-----|-------|-------|---------|
| Rabbit MQ | 1   | 1     | 1     | 1       |
| Rocket MQ | 1   | 1     | 1     | 1       |
| Kafka     | 1   | 1     | 1     | 1       |
| Rabbit MQ | 4   | 1     | 1     | 1       |
| Rocket MQ | 4   | 1     | 1     | 1       |
| Kafka     | 4   | 1     | 1     | 1       |
| Rabbit MQ | 4   | 2     | 1     | 1       |
| Rocket MQ | 4   | 2     | 1     | 1       |
| Kafka     | 4   | 2     | 1     | 1       |
| Rabbit MQ | 4   | 1     | 2     | 1       |
| Rocket MQ | 4   | 1     | 2     | 1       |
| Kafka     | 4   | 1     | 2     | 1       |
| Rabbit MQ | 4   | 2     | 2     | 1       |
| Rocket MQ | 4   | 2     | 2     | 1       |
| Kafka     | 4   | 2     | 2     | 1       |

# 三、测试结论
## 单例
1000条，每条1024B做两轮测试
RabbitMQ 测试结果: {duration=7.664, ops=130.48016701461378, failCount=58, avgLatency=6.45859872611465, totalMessages=1000, successCount=942, throughput=122.91231732776619, mqType=RabbitMQ}
RocketMQ 测试结果: {duration=6.169, ops=162.10082671421625, failCount=46, avgLatency=6.156184486373165, totalMessages=1000, successCount=954, throughput=154.6441886853623, mqType=RocketMQ}
Kafka 测试结果: {duration=6.393, ops=156.42108556233381, failCount=51, avgLatency=6.326659641728135, totalMessages=1000, successCount=949, throughput=148.4436101986548, mqType=Kafka}

RabbitMQ 测试结果: {duration=7.559, ops=132.29263130043657, failCount=61, avgLatency=6.533546325878595, totalMessages=1000, successCount=939, throughput=124.22278079110993, mqType=RabbitMQ}
RocketMQ 测试结果: {duration=6.351, ops=157.4555188159345, failCount=52, avgLatency=6.34704641350211, totalMessages=1000, successCount=948, throughput=149.2678318375059, mqType=RocketMQ}
Kafka 测试结果: {duration=6.469, ops=154.58339774308237, failCount=51, avgLatency=6.528977871443625, totalMessages=1000, successCount=949, throughput=146.6996444581852, mqType=Kafka}

100000条，每条1024B
RabbitMQ 测试结果: {duration=931.684, ops=107.33252905491563, failCount=4993, avgLatency=9.3028934710074, totalMessages=100000, successCount=95007, throughput=101.97341587920369, mqType=RabbitMQ}
RocketMQ 测试结果: {duration=1376.881, ops=72.62791773581013, failCount=4886, avgLatency=13.738461214963097, totalMessages=100000, successCount=95114, throughput=69.07931767523846, mqType=RocketMQ}
Kafka 测试结果: {duration=984.909, ops=101.53222277388063, failCount=4966, avgLatency=9.849411789464822, totalMessages=100000, successCount=95034, throughput=96.49013259092972, mqType=Kafka}

## npnc
1000条，每条1024B
RabbitMQ 完整测试结果: {duration=4.023, queueCount=1, ops=248.5707183693761, consumerCount=2, producerCount=2, totalMessages=1000, mqType=RabbitMQ}
RocketMQ 完整测试结果: {duration=3.143, queueCount=1, ops=318.16735602927145, consumerCount=2, producerCount=2, totalMessages=1000, mqType=RocketMQ}
Kafka 完整测试结果: {duration=3.066, queueCount=1, ops=326.1578604044358, consumerCount=2, producerCount=2, totalMessages=1000, mqType=Kafka}
RabbitMQ 高并发测试结果: {duration=1.663, queueCount=1, ops=601.3229104028864, consumerCount=4, producerCount=4, totalMessages=1000, mqType=RabbitMQ}
RocketMQ 高并发测试结果: {duration=1.618, queueCount=1, ops=618.0469715698392, consumerCount=4, producerCount=4, totalMessages=1000, mqType=RocketMQ}
Kafka 高并发测试结果: {duration=1.701, queueCount=1, ops=587.8894767783656, consumerCount=4, producerCount=4, totalMessages=1000, mqType=Kafka}


100000条，每条1024B
RabbitMQ 完整测试结果: {duration=642.558, queueCount=1, ops=155.62797443966147, consumerCount=2, producerCount=2, totalMessages=100000, mqType=RabbitMQ}
RocketMQ 完整测试结果: {duration=536.972, queueCount=1, ops=186.22944958023882, consumerCount=2, producerCount=2, totalMessages=100000, mqType=RocketMQ}
Kafka 完整测试结果: {duration=307.078, queueCount=1, ops=325.65016054552916, consumerCount=2, producerCount=2, totalMessages=100000, mqType=Kafka}
RabbitMQ 高并发测试结果: {duration=336.845, queueCount=1, ops=296.8724487523935, consumerCount=4, producerCount=4, totalMessages=100000, mqType=RabbitMQ}
RocketMQ 高并发测试结果: {duration=239.663, queueCount=1, ops=417.2525588013168, consumerCount=4, producerCount=4, totalMessages=100000, mqType=RocketMQ}
Kafka 高并发测试结果: {duration=362.823, queueCount=1, ops=275.6164851732112, consumerCount=4, producerCount=4, totalMessages=100000, mqType=Kafka}
