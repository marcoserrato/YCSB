/*
 *
 */

package site.ycsb.generator;

import java.util.concurrent.ThreadLocalRandom;


/**
 * The indexing service uses kafka's partioning scheme to assure that multiple
 * processes don't handle the same aggregates. We need a generator that can mirror
 * this behavior at a thread leve. This generator should uniformly return a value
 * in an interval [lb, ub] within a larger bucket split by thread
 * thread 1 [lb, ub], thread 2 [lb + lb, ub + lb] ..
 */

public class BucketGenerator extends NumberGenerator {
  private final long lb, ub, interval, threadcount;

  public BucketGenerator(long lb, long ub, long threadcount) {
    this.lb = lb;
    this.ub = ub;
    this.threadcount = threadcount;
    interval = (this.ub - this.lb) / this.threadcount + 1;
  }

  @Override
  public Long nextValue() {
    long threadid = Thread.currentThread().getId() % threadcount;

    long ret = Math.abs(ThreadLocalRandom.current().nextLong()) % interval + (interval * threadid);
    setLastValue(ret);

    return ret;
  }

  @Override
  public double mean() {
    return ((lb + (long) ub)) / 2.0;
  }
}
