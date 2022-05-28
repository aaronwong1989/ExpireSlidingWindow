package org.happy.esw;

import java.security.Key;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
class ExpireSlidingWindowTest {

  ExpireSlidingWindow<String, Integer> esw;


  @BeforeEach
  void beforeEach() {
    esw = new ExpireSlidingWindow<>(
        "CMC",
        5,
        1,
        TimeUnit.SECONDS,
        (key, value) -> log.info("{} expired!", key),
        (key, value) -> log.info("send to MQ : <{},{}> ", key, value)
    );
    esw.setup();
  }

  @Test
  void example() throws InterruptedException {
    String key = "key";
    // 只会被发送5条，其他5条进入担保机制
    for (int i = 1; i < 10; i++) {
      if (esw.put(key + i, i)) {
        log.info("send message: {},{}", key + i, esw.get(key + i));
      }
    }
    TimeUnit.MILLISECONDS.sleep(1005);

    // 10条都会发送，因为消息处理后移除的窗口
    for (int i = 10; i < 20; i++) {
      if (esw.put(key + i, i)) {
        log.info("send message: {},{}", key + i, esw.get(key + i));
        // TODO 消息处理
        esw.remove(key + i);
      }
    }
    TimeUnit.MILLISECONDS.sleep(1005);
  }


  @Test
  void setup() throws InterruptedException {
    esw.put("hello", 1);
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  void put() throws InterruptedException {
    assert esw.put("hello", 1);
    assert esw.size() == 1;
    assert esw.get("hello") == 1;
    TimeUnit.MILLISECONDS.sleep(10);

    // 相同KEY值被更新
    assert esw.put("hello", 2);
    assert esw.size() == 1;
    assert esw.get("hello") == 2;
    TimeUnit.MILLISECONDS.sleep(10);

    assert esw.put("hello2", 2);
    assert esw.size() == 2;
    assert esw.get("hello2") == 2;
    TimeUnit.MILLISECONDS.sleep(10);

    assert esw.put("hello3", 3);
    assert esw.size() == 3;
    assert esw.get("hello3") == 3;
    TimeUnit.MILLISECONDS.sleep(10);

    assert esw.put("hello4", 4);
    assert esw.size() == 4;
    assert esw.get("hello4") == 4;
    TimeUnit.MILLISECONDS.sleep(10);

    assert esw.put("hello5", 5);
    assert esw.size() == 5;
    assert esw.get("hello5") == 5;
    TimeUnit.MILLISECONDS.sleep(10);

    // 达到临界值时，不支持重新设置已有KEY
    assert !esw.put("hello5", 6);
    assert esw.size() == 5;
    assert esw.get("hello5") == 5;

    // 达到临界值时，设置新KEY会走担保handler
    assert !esw.put("hello6", 6);
    assert esw.size() == 5;
    assert esw.get("hello6") == null;

    TimeUnit.SECONDS.sleep(2);
    assert esw.size() == 0;
    assert esw.get("hello5") == null;
  }

  @Test
  void putToAlive() throws InterruptedException {
    // 重新设置会续期
    esw.put("hello", 1);
    TimeUnit.MILLISECONDS.sleep(700);
    esw.put("hello", 1);
    TimeUnit.MILLISECONDS.sleep(700);
    // 注意观察日志，此时已经距离上面设置"hello"超过了1s，但是不会打印过期
    log.info(".........mark line.........");
    TimeUnit.MILLISECONDS.sleep(400);
    // 现在连续sleep超过1s后，会过期了。
  }

  @Test
  void remove() throws InterruptedException {
    esw.put("hello1", 1);
    esw.put("hello2", 1);
    esw.put("hello3", 1);
    assert esw.size() == 3;
    assert esw.remove("hello3") == 1;
    assert esw.size() == 2;
    TimeUnit.MILLISECONDS.sleep(1200);
    assert esw.size() == 0;
  }

  @Test
  void containsKey() throws InterruptedException {
    esw.put("hello1", 1);
    assert esw.containsKey("hello1");
    TimeUnit.MILLISECONDS.sleep(1005);
    assert !esw.containsKey("hello1");
  }

}