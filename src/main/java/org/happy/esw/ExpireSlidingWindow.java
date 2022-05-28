package org.happy.esw;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

/**
 * <h3>元素会过期的"滑动窗口"</h3> <br>
 * <p>
 * 注意：窗口大小为初始化后的固定值不会变化，当窗口达到临界值时，不支持相同Key的元素更新。<br>
 * <p>
 * 适用用采用"窗口机制"控制消息处理速度的场景，窗口机制的优势是能自动调节生产者与消费者处理能力。使用时，处理消息前将消息put到窗口，消息被成功（或失败）处理后从窗口移除。<br>
 * <p>
 * <b>功能点：</b><br>
 * 1.支持设定一个过期时间，元素在超过一定时长后，会自动从窗口清除，采用DelayQueue实现；<br>
 * <p>
 * 2.清除过期元素支持自定义回调函数来做清除后处理工作，比如更新数据库状态；<br>
 * <p>
 * 3.当窗口达到临界值大小时，不接受新的元素，且会触发自定义回调操作，可用于消息处理的担保机制。<br>
 *
 * @author huangzhonghui
 */
@Slf4j
public class ExpireSlidingWindow<K, V> {

  /**
   * 滑动窗口名称
   */
  private final String name;
  /**
   * 滑动窗口大小
   */
  private final int windowCapacity;
  /**
   * 滑动窗口中元素在此毫秒数后会被自动清除
   */
  private final long delayedMillis;
  /**
   * 存储
   */
  private final ConcurrentHashMap<K, V> cache;
  /**
   * 延迟队列，实现过期
   */
  private final DelayQueue<DelayItem<K>> delayQueue;
  /**
   * 元素超时的回调函数
   */
  private final Callback<K, V> expireCallback;
  /**
   * 滑动窗口已满触发限速的回调函数
   */
  private final Callback<K, V> rateLimitingCallback;

  private final AtomicInteger size = new AtomicInteger(0);
  private final AtomicBoolean setup = new AtomicBoolean(false);

  private final transient ReentrantLock lock = new ReentrantLock();

  public ExpireSlidingWindow(String name, int windowCapacity, long delayedTime, TimeUnit unit,
      Callback<K, V> expireCallback,
      Callback<K, V> rateLimitingCallback) {
    this.name = name;
    this.windowCapacity = windowCapacity;
    this.delayedMillis = TimeUnit.MILLISECONDS.convert(delayedTime, unit);
    cache = new ConcurrentHashMap<K, V>(windowCapacity);
    delayQueue = new DelayQueue<>();
    this.expireCallback = expireCallback;
    this.rateLimitingCallback = rateLimitingCallback;
  }

  /**
   * 启动过期检查线程
   */
  public void setup() {
    if (!setup.get()) {
      Thread expireCheckThread = new Thread(this::expireCheck);
      expireCheckThread.setDaemon(true);
      expireCheckThread.setName("ExpireCacheCheckThread-" + name);
      expireCheckThread.start();
      setup.set(true);
    }
  }

  /**
   * 向窗口中添加一个元素，注意kv均不能为空
   *
   * @param key   key not null
   * @param value value not null
   */
  public void put(K key, V value) {
    if (!this.setup.get()) {
      throw new RuntimeException("使用滑动窗口前，须先执行setup()方法，该方法仅需执行一次!");
    }
    if (key == null || value == null) {
      return;
    }
    // 活动窗口未满
    if (this.size.get() < this.windowCapacity) {
      this.lock.lock();
      try {
        V oldValue = cache.put(key, value);
        if (oldValue != null) {
          for (DelayItem<K> item : delayQueue) {
            if (item.getItem().equals(key)) {
              item.refresh();
              break;
            }
          }
        } else {
          // 新元素
          this.delayQueue.offer(new DelayItem<>(key, this.delayedMillis, TimeUnit.MILLISECONDS));
          this.size.incrementAndGet();
        }
      } finally {
        this.lock.unlock();
      }
    }
    // 滑动窗口已满，且具备回调函数时，如果添加的是新元素，则执行回调函数，做担保处理
    else if (!this.containsKey(key) && this.rateLimitingCallback != null) {
      try {
        log.info("滑动窗口已满，执行rateLimitingCallback.handle() for :{}", key);
        this.rateLimitingCallback.handle(key, value);
      } catch (Exception e) {
        log.error("rateLimitingCallback.handle() cause unknown exception", e);
      }
    }
  }

  /**
   * 删除制定Key对应的元素
   */
  public V remove(K key) {
    this.lock.lock();
    try {
      V value = this.cache.remove(key);
      if (value != null) {
        this.delayQueue.remove(new DelayItem<>(key, 0L, TimeUnit.MILLISECONDS));
        this.size.decrementAndGet();
      }
      return value;
    } finally {
      this.lock.unlock();
    }
  }

  /**
   * 获取指定Key对应的元素
   */
  public V get(K key) {
    return this.cache.get(key);
  }

  public boolean containsKey(K key) {
    return this.cache.containsKey(key);
  }

  public int size() {
    return this.size.get();
  }

  /**
   * 真正的失效检测
   */
  private void expireCheck() {
    for (; ; ) {
      try {
        DelayItem<K> delayItem = this.delayQueue.take();
        this.lock.lock();
        try {
          V v = this.cache.remove(delayItem.getItem());
          if (v != null) {
            this.size.decrementAndGet();
          }
        } finally {
          this.lock.unlock();
        }
        if (this.expireCallback != null) {
          try {
            log.info("滑动窗中元素因过期而被清理，执行expireCallback.handle() for :{}", delayItem.getItem());
            this.expireCallback.handle(delayItem.getItem(), this.cache.get(delayItem.getItem()));
          } catch (Exception e) {
            log.error("expireCallback.handle() cause unknown exception", e);
          }
        }
      } catch (Exception e) {
        log.error("expireCheck() cause unknown exception", e);
      }
    }
  }
}