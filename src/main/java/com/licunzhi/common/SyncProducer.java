package com.licunzhi.common;

import java.util.Scanner;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author lcz
 * @date 2022/02/08 20:51
 * 同步操作
 */
public class SyncProducer {

  public static void main(String[] args) throws Exception {
    // 实例化消息生产者Producer
    DefaultMQProducer producer = new DefaultMQProducer("LICUNZHI");
    // 设置NameServer的地址
    producer.setNamesrvAddr("127.0.0.1:9876");
    // 启动Producer实例
    producer.start();

    // topic存在性判断
    try {
      producer.createTopic(producer.getCreateTopicKey(),"LCZ_ORDER_TEST", 1);
    } catch (Exception e) {
      e.printStackTrace();
    }

    Scanner scanner = new Scanner(System.in);
    System.out.println("请输入需要发送的消息：");
    String flag = scanner.nextLine();
    System.out.println("请输入需要发送的消息tag属性：");
    String tag = scanner.nextLine();
    try {
      while (!flag.equalsIgnoreCase("n")) {
        // 创建消息，并指定Topic，Tag和消息体
        Message msg = new Message("LCZ_ORDER_TEST" /* Topic */,
            tag /* Tag */,
            (flag).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
        );
        // 发送消息到一个Broker
        SendResult sendResult = producer.send(msg);
        // 通过sendResult返回消息是否成功送达
        System.out.printf("%s%n", sendResult);

        System.out.println("是否继续发送消息内容？（n or msg）");
        flag = scanner.nextLine();
        System.out.println("请输入需要发送的消息tag属性：");
        tag = scanner.nextLine();
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // 如果不再发送消息，关闭Producer实例。
      producer.shutdown();
    }
  }
}
