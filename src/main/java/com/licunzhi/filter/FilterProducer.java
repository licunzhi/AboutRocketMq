package com.licunzhi.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author lcz
 * @date 2022/02/09 14:50
 */
public class FilterProducer {

  public static void main(String[] args) throws Exception {

    DefaultMQProducer producer = new DefaultMQProducer("LICUNZHI_FILTER_MESSAGE");
    producer.setNamesrvAddr("127.0.0.1:9876");
    producer.start();

    try {
      producer.createTopic(producer.getCreateTopicKey(),"FILTER_MESSAGE_TOPIC", 4);
    } catch (Exception e) {
      e.printStackTrace();
    }

    for (int i = 0; i < 10; i++) {
      Message msg = new Message("FILTER_MESSAGE_TOPIC",
          ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
      );
      // 设置一些属性
      msg.putUserProperty("a", String.valueOf(i));
      SendResult sendResult = producer.send(msg);

      // 通过sendResult返回消息是否成功送达
      System.out.printf("%s%n", sendResult);
    }

    producer.shutdown();

  }

}
