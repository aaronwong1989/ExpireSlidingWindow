package org.happy.mq;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.happy.esw.ExpireSlidingWindow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class EswMessageQueueTest {

  ExpireSlidingWindow<String, Message<String>> esw;
  BlockingQueue<Message<String>> queue;
  EswMessageQueue<String, Message<String>> eswMQ;

  ExecutorService producer;
  ExecutorService consumer;

  @BeforeEach
  void beforeEach() {
    int windowSize = 5;
    long aliveTime = 1;
    esw = new ExpireSlidingWindow<>(
        "CMC",
        windowSize,
        aliveTime,
        TimeUnit.SECONDS,
        (key, value) -> log.info("{} expired!", key),
        (key, value) -> log.info("send to MQ : <{},{}> ", key, value)
    );
    esw.setup();
    // 容量可以比滑动窗口大
    queue = new LinkedBlockingQueue<>(windowSize + 1);
    eswMQ = new EswMessageQueue<>(queue, esw);
    producer = Executors.newCachedThreadPool();
    consumer = Executors.newCachedThreadPool();
  }

  @Test
  void putAndTakeAndSend() throws InterruptedException {
    // 测试连续投递6条消息，第6条会采用担保机制
    String id = "01";
    String phoneNumber = "186000000" + id;
    String content = "hello world " + phoneNumber;
    eswMQ.put(new Message<>(id, phoneNumber, content));
    assert eswMQ.queueSize() == 1;
    assert eswMQ.eswSize() == 1;
    TimeUnit.MILLISECONDS.sleep(10);

    id = "02";
    phoneNumber = "186000000" + id;
    content = "hello world " + phoneNumber;
    eswMQ.put(new Message<>(id, phoneNumber, content));
    assert eswMQ.queueSize() == 2;
    assert eswMQ.eswSize() == 2;
    TimeUnit.MILLISECONDS.sleep(10);

    id = "03";
    phoneNumber = "186000000" + id;
    content = "hello world " + phoneNumber;
    eswMQ.put(new Message<>(id, phoneNumber, content));
    assert eswMQ.queueSize() == 3;
    assert eswMQ.eswSize() == 3;
    TimeUnit.MILLISECONDS.sleep(10);

    id = "04";
    phoneNumber = "186000000" + id;
    content = "hello world " + phoneNumber;
    eswMQ.put(new Message<>(id, phoneNumber, content));
    assert eswMQ.queueSize() == 4;
    assert eswMQ.eswSize() == 4;
    TimeUnit.MILLISECONDS.sleep(10);

    id = "05";
    phoneNumber = "186000000" + id;
    content = "hello world " + phoneNumber;
    eswMQ.put(new Message<>(id, phoneNumber, content));
    assert eswMQ.queueSize() == 5;
    assert eswMQ.eswSize() == 5;
    TimeUnit.MILLISECONDS.sleep(10);

    // 因窗口已满，该消息未入队
    id = "06";
    phoneNumber = "186000000" + id;
    content = "hello world " + phoneNumber;
    eswMQ.put(new Message<>(id, phoneNumber, content));
    TimeUnit.MILLISECONDS.sleep(20);
    assert eswMQ.queueSize() == 5;
    assert eswMQ.eswSize() == 5;

    // 令窗口中消息过期
    TimeUnit.MILLISECONDS.sleep(1000);
    assert eswMQ.eswSize() == 0;

    // 窗口元素过期，有多余空间，队列可继续投递
    id = "07";
    phoneNumber = "186000000" + id;
    content = "hello world " + phoneNumber;
    log.info("投递消息07。");
    eswMQ.put(new Message<>(id, phoneNumber, content));
    assert eswMQ.queueSize() == 6;
    assert eswMQ.eswSize() == 1;
    TimeUnit.MILLISECONDS.sleep(20);

    // 队列已溢出，阻塞
    producer.execute(() -> {
      String id1 = "08";
      String phoneNumber1 = "186000000" + id1;
      String content1 = "hello world " + phoneNumber1;
      try {
        // 阻塞
        log.info("投递消息08,但被阻塞（入ESW成功，入队列阻塞）。");
        eswMQ.put(new Message<>(id1, phoneNumber1, content1));
        log.info("投递消息08,成功。");
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    // 消息8入窗口成功，但入eswMQ被阻塞
    TimeUnit.MILLISECONDS.sleep(5);
    assert eswMQ.eswSize() == 2;

    // 一条消息消费后，发送阻塞解除
    consumer.execute(() -> {
      try {
        TimeUnit.MILLISECONDS.sleep(500);
        log.info("消费全部1+6条消息（窗口中5条已过期，但队列中存在）");
        // 一条消息消费后，消息08投递成功。
        log.info("eswMQ.eswSize(): {}, eswMQ.queueSize(): {}", eswMQ.eswSize(), eswMQ.queueSize());
        eswMQ.takeAndSend(message -> {
          log.info("eswMQ.takeAndSend() ：{}", message);
          return 1;
        }, 1);

        for (int i = 0; i < 6; i++) {
          eswMQ.takeAndSend(message -> {
            log.info("eswMQ.takeAndSend() ：{}", message);
            return 1;
          }, 1);
          log.info("eswMQ.eswSize(): {}, eswMQ.queueSize(): {}", eswMQ.eswSize(), eswMQ.queueSize());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    TimeUnit.MILLISECONDS.sleep(1000);
  }

  @Getter
  @Setter
  @ToString
  static class Message<String> implements KvMessage<String> {

    private final String id;
    private final String phoneNumber;
    private final String content;

    public Message(String id, String phoneNumber, String content) {
      this.id = id;
      this.phoneNumber = phoneNumber;
      this.content = content;
    }

    @Override
    public String getKey() {
      return this.getId();
    }
  }
}