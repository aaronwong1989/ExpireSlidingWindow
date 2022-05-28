package org.happy.esw;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 封装用于DelayQueue的数据
 *
 * @author huangzhonghui
 */
public class DelayItem<T> implements Delayed {

  private final T item;
  /**
   * 可存活时长，单位 TimeUnit.MILLISECONDS
   */
  private final long aliveTime;

  /**
   * 创建或更新时间
   */
  private long creationTime = System.currentTimeMillis();

  /**
   * 构建一个延迟元素
   *
   * @param item      数据
   * @param aliveTime 存活时长
   * @param unit      存活时长的时间单位
   */
  public DelayItem(T item, long aliveTime, TimeUnit unit) {
    this.item = item;
    this.aliveTime = aliveTime > 0 ? TimeUnit.MILLISECONDS.convert(aliveTime, unit) : aliveTime;
  }


  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(aliveTime - (System.currentTimeMillis() - creationTime), TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed o) {
    if (o == this) {
      return 0;
    }
    // FIFO
    long d = (this.getDelay(TimeUnit.NANOSECONDS) - o.getDelay(TimeUnit.NANOSECONDS));
    return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
  }

  public T getItem() {
    return item;
  }

  void refresh() {
    this.creationTime = System.currentTimeMillis();
  }

  @Override
  public String toString() {
    return "DelayItem{" +
        "item=" + item +
        ", creationTime=" + creationTime +
        ", aliveTime=" + aliveTime +
        '}';
  }
}
