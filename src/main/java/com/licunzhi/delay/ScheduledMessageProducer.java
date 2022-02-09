package com.licunzhi.delay;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class ScheduledMessageProducer {

  public static void main(String[] args) throws Exception {
    // 实例化一个生产者来产生延时消息
    DefaultMQProducer producer = new DefaultMQProducer("LICUNZHI_SCHEDULE_MESSAGE");
    producer.setNamesrvAddr("127.0.0.1:9876");
    // 启动生产者
    producer.start();

    // topic存在性判断
    try {
      producer.createTopic(producer.getCreateTopicKey(),"DELAY_MESSAGE_TOPIC", 4);
    } catch (Exception e) {
      e.printStackTrace();
    }

    int totalMessagesToSend = 100;
    for (int i = 0; i < totalMessagesToSend; i++) {
      Message message = new Message("DELAY_MESSAGE_TOPIC", ("Hello scheduled message " + i).getBytes());
      // 设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
      message.setDelayTimeLevel(3);
      // 发送消息
      producer.send(message);
      System.out.println("Hello scheduled message " + i);
    }
    // 关闭生产者
    producer.shutdown();
  }
}