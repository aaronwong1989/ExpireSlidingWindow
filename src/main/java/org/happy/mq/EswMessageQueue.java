package org.happy.mq;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.happy.esw.ExpireSlidingWindow;

/**
 * 带滑动窗口的消息队列
 *
 * @param <K> 消息的Key类型
 * @param <V> 消息的Value类型
 */
@Slf4j
public class EswMessageQueue<K, V extends KvMessage<K>> {

  private final BlockingQueue<V> queue;
  private final ExpireSlidingWindow<K, V> esw;
  private final Lock putLock = new ReentrantLock();

  public EswMessageQueue(BlockingQueue<V> queue, ExpireSlidingWindow<K, V> esw) {
    this.queue = queue;
    this.esw = esw;
    this.esw.setup();
  }

  /**
   * 投递消息。
   * <p>
   * 滑动窗口未满时会阻塞方法，直到消息成功加入到窗口。
   * <p>
   * 滑动窗口满时，方法不阻塞，消息采用担保机制处理。
   */
  public void put(V value) throws InterruptedException {
    putLock.lock();
    try {
      if (esw.put(value.getKey(), value)) {
        queue.put(value);
      }
    } finally {
      putLock.unlock();
    }
  }

  /**
   * 获取并发送消息，如果消息发送成功，则会从滑动窗口中移出，否则会占用窗口，抑制消息投递。
   * <p>
   * 队列中没有消息时，该方法会阻塞。
   *
   * @param sender      传入的自定义消息发送函数
   * @param successFlag sender返回此值时标识发送成功，其他为发送失败
   * @return 消息发送结果码
   * @throws Exception 异常
   */
  public int takeAndSend(MessageSender<V> sender, int successFlag) throws Exception {
    V message = queue.take();
    int result = sender.send(message);
    if (successFlag == result) {
      esw.remove(message.getKey());
    }
    return result;
  }

  /**
   * 消息队列的消息数量
   */
  public int queueSize() {
    return this.queue.size();
  }

  /**
   * 滑动窗口的消息数量
   */
  public int eswSize() {
    return this.esw.size();
  }
}
