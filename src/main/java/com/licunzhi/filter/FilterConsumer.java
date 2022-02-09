package com.licunzhi.filter;

import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author lcz
 * @date 2022/02/09 14:52
 */
public class FilterConsumer {

  public static void main(String[] args) throws Exception {
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("LICUNZHI_FILTER_MESSAGE");
    consumer.setNamesrvAddr("127.0.0.1:9876");

    // 只有订阅的消息有这个属性a, a >=0 and a <= 3
    consumer.subscribe("FILTER_MESSAGE_TOPIC", MessageSelector.bySql("a between 0 and 3"));
    consumer.registerMessageListener(new MessageListenerConcurrently() {
      @Override
      public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
          ConsumeConcurrentlyContext context) {
        // licunzhi 消息处理
        MessageExt messageExt = msgs.get(0);
        try {
          String messageBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
          System.out.println(messageBody);
        } catch (UnsupportedEncodingException e) {
          System.out.println("反序列化消息失败...");
          e.printStackTrace();
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
      }
    });
    consumer.start();

  }
}
