package org.astraea.concurrent;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class offers a simple way to manage the threads. All threads added to builder are not
 * executing right now. Instead, all threads are starting when the pool is built.
 */
public interface ThreadPool extends AutoCloseable {

  /** close all running threads. */
  @Override
  void close();

  static Builder builder() {
    return new Builder();
  }

  @FunctionalInterface
  interface Executor {
    /**
     * @throws InterruptedException This is an expected exception if your executor needs to call
     *     blocking method. This exception is not printed to console.
     */
    void execute() throws InterruptedException;

    /** cleanup this executor. */
    default void cleanup() {}

    /**
     * If this executor is in blocking mode, this method offers a way to wake up executor to close.
     */
    default void wakeup() {}
  }

  class Builder {
    private final List<Executor> executors = new ArrayList<>();
    private final AtomicInteger loop = new AtomicInteger(-1);

    private Builder() {}

    public Builder executor(Executor executor) {
      return executors(List.of(executor));
    }

    public Builder executors(Collection<Executor> executors) {
      this.executors.addAll(Objects.requireNonNull(executors));
      return this;
    }

    /**
     * @param loop the count to execute thread. For example, all threads are executed only once if
     *     the loop is equal to 1. By default, loop is infinite.
     * @return this builder.
     */
    public Builder loop(int loop) {
      this.loop.set(loop);
      return this;
    }

    public ThreadPool build() {
      var closed = new AtomicBoolean(false);
      var latch = new CountDownLatch(executors.size());
      var service = Executors.newFixedThreadPool(executors.size());
      executors.forEach(
          executor ->
              service.execute(
                  () -> {
                    try {
                      var count = 0;
                      while (!closed.get() && (loop.get() <= 0 || count < loop.get())) {
                        executor.execute();
                        count += 1;
                      }
                    } catch (InterruptedException e) {
                      // swallow
                    } finally {
                      try {
                        executor.cleanup();
                      } finally {
                        latch.countDown();
                      }
                    }
                  }));
      return () -> {
        service.shutdown();
        closed.set(true);
        executors.forEach(Executor::wakeup);
        try {
          latch.await();
        } catch (InterruptedException e) {
          // swallow
        }
      };
    }
  }
}