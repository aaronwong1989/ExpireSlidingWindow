package mq;

/**
 * 消息发送函数
 *
 * @author huangzhonghui
 */
@FunctionalInterface
public interface MessageSender<V> {

  /**
   * 发送消息
   *
   * @param message 待发送消息
   * @return 消息发送状态
   */
  int send(V message) throws Exception;

}
