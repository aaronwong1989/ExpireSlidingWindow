package org.happy.mq;

/**
 * 消息发送函数
 *
 * @author huangzhonghui
 */
@FunctionalInterface
public interface MessageSender<T> {

  /**
   * 发送消息
   *
   * @param message 待发送消息
   * @return 消息发送状态
   */
  int send(T message) throws Exception;

}
