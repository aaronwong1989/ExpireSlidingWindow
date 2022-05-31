package org.happy.mq;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.happy.esw.ExpireSlidingWindow;
import org.happy.mq.EswMessageQueueTest.Message;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 6, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(8)
@Slf4j
public class EswMQBenchmarkTest {

  EswMessageQueue<String, Message<String>> eswMQ;
  ExpireSlidingWindow<String, Message<String>> esw;

  @Setup(Level.Trial)
  public void setup() {
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
    BlockingQueue<Message<String>> queue = new LinkedBlockingQueue<>(windowSize + 1);
    eswMQ = new EswMessageQueue<>(queue, esw);
  }

  Lock benchLock = new ReentrantLock();

  @Benchmark
  public void benchmarkOfBenchmark() {
    benchLock.lock();
    try {
      RandomStringUtils.randomAlphabetic(24);
      RandomStringUtils.randomAlphabetic(24);
    } finally {
      benchLock.unlock();
    }
  }

  Lock lockEsW = new ReentrantLock();

  @Benchmark
  public void benchOfEsw() {
    lockEsW.lock();
    try {
      String key = RandomStringUtils.randomAlphabetic(24);
      String value = RandomStringUtils.randomAlphabetic(24);
      Message<String> message = new Message<>(key, "", value);
      esw.put(key, message);
      esw.remove(key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lockEsW.unlock();
    }
  }

  Lock lock = new ReentrantLock();

  @Benchmark
  public void benchOfEswMQ() {
    lock.lock();
    try {
      String key = RandomStringUtils.randomAlphabetic(24);
      String value = RandomStringUtils.randomAlphabetic(24);
      Message<String> message = new Message<>(key, "", value);
      eswMQ.put(message);
      eswMQ.takeAndSend(msg -> 1, 1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }
}
