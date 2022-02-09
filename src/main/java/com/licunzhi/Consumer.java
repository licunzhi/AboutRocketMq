package com.licunzhi;

import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author lcz
 * @date 2022/02/08 21:05
 */
public class Consumer {

  public static void main(String[] args) throws InterruptedException, MQClientException {

    // 实例化消费者
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("LICUNZHI_CONSUMER");

    // 设置NameServer的地址
    consumer.setNamesrvAddr("127.0.0.1:9876");

    // 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
    // 定制化tag   *
    String tags = "tagA || tagB";

    consumer.subscribe("LCZ_ORDER_TEST", tags);
    // 注册回调实现类来处理从broker拉取回来的消息
    consumer.registerMessageListener(new MessageListenerConcurrently() {
      @Override
      public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
          ConsumeConcurrentlyContext context) {
//        System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);

        // licunzhi 消息处理
        MessageExt messageExt = msgs.get(0);
        try {
          String messageBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
          System.out.println(messageBody);
        } catch (UnsupportedEncodingException e) {
          System.out.println("反序列化消息失败...");
          e.printStackTrace();
        }

        // 标记该消息已经被成功消费
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
      }
    });
    // 启动消费者实例
    consumer.start();
    System.out.printf("Consumer Started.%n");
  }
}
