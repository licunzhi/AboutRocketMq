package com.licunzhi.txm;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @author lcz
 * @date 2022/02/09 15:25
 */
public class TransactionListenerImpl implements TransactionListener {

  private AtomicInteger transactionIndex = new AtomicInteger(0);
  private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

  @Override
  public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
    try {
      System.out.println("消息内容为：" + new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET));
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    int value = transactionIndex.getAndIncrement();
    int status = value % 3;
    System.out.println("设置状态为： " + status);
    localTrans.put(msg.getTransactionId(), status);
    return LocalTransactionState.UNKNOW;
  }

  @Override
  public LocalTransactionState checkLocalTransaction(MessageExt msg) {
    Integer status = localTrans.get(msg.getTransactionId());
    if (null != status) {
      switch (status) {
        case 0:
          return LocalTransactionState.UNKNOW;
        case 1:
          return LocalTransactionState.COMMIT_MESSAGE;
        case 2:
          return LocalTransactionState.ROLLBACK_MESSAGE;
      }
    }
    return LocalTransactionState.COMMIT_MESSAGE;
  }
}
