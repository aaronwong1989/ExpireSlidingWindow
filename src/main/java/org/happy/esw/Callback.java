package org.happy.esw;

/**
 * 回调函数
 *
 * @author huangzhonghui
 */
@FunctionalInterface
public interface Callback<K, V> {

  /**
   * 回调处置方法
   *
   * @param k key
   * @param v value
   * @throws Exception 异常
   */
  void handle(K k, V v) throws Exception;

}
