package com.licunzhi.parseFile;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author lcz
 * @date 2022/02/08 14:36
 *
 * 解析消费对列中存储的数据内容
 * hash(tag)|size|offset(commitLog)
 *  42 274 4864092459
 *  42 274 4864113354
 *  42 274 4864114700
 */
public class ParseConsumeQueue {

  public static void main(String[] args) throws IOException {
    System.out.println("hash(tag)|size|offset(commitLog)");
    decodeCQ(new File("00000000000000000000"));

  }

  static void decodeCQ(File consumeQueue) throws IOException {
    FileInputStream fis = new FileInputStream(consumeQueue);
    DataInputStream dis = new DataInputStream(fis);

    long preTag = 0;
    long count = 1;
    while (true) {
      long offset = dis.readLong();
      int size = dis.readInt();
      long tag = dis.readLong();

      if (size == 0) {
        break;
      }
      preTag = tag;
      System.out.printf(" %d %d %d\n", tag, size, offset);
    }
    fis.close();
  }
}
