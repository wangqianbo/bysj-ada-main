package ict.ada.gdb.util;

/**
 * 
 * A tool for collecting min, max, avg for a series of time cost values
 * 
 */
public class TimeStat {

  private long max = Long.MIN_VALUE;
  private long min = Long.MAX_VALUE;
  private long total = 0;
  private long count = 0;

  public void add(long v) {
    total += v;
    count++;
    if (v > max) max = v;
    if (v < min) min = v;
  }

  public double getAvg() {
    if (count == 0) return 0;
    else return total * 1.0 / count;
  }

  public long getMax() {
    return max;
  }

  public long getMin() {
    return min;
  }

  public long getTotal() {
    return total;
  }

  public long getCount() {
    return count;
  }

}
