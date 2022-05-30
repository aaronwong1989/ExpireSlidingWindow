package org.happy.esw;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
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

@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 6, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(8)
@Slf4j
public class EswBenchmarkTest {

  ExpireSlidingWindow<String, String> esw;

  @Setup(Level.Trial)
  public void setup() {
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

  Lock lock = new ReentrantLock();

  @Benchmark
  public void benchOfEsw() {
    lock.lock();
    try {
      String key = RandomStringUtils.randomAlphabetic(24);
      String value = RandomStringUtils.randomAlphabetic(24);
      esw.put(key, value);
      esw.remove(key);
    } finally {
      lock.unlock();
    }
  }
}
