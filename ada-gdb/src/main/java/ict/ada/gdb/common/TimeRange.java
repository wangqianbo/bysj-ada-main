package ict.ada.gdb.common;

/**
 * Represent a time interval.
 * <p>
 * This is a Value Object.
 */
public class TimeRange {

  public static final TimeRange ANY_TIME = new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE);

  private final long startSecInclusive;// in seconds
  private final long endSecExclusive;// in seconds

  /**
   * 
   * @param startSecInclusive
   *          in seconds
   * @param endSecExclusive
   *          in seconds
   */
  public TimeRange(long startSecInclusive, long endSecExclusive) {
    if (startSecInclusive > endSecExclusive)
      throw new IllegalArgumentException("startInclusive=" + startSecInclusive + " endExclusive="
          + endSecExclusive);
    this.startSecInclusive = startSecInclusive;
    this.endSecExclusive = endSecExclusive;
  }

  /**
   * @return start time in seconds
   */
  public long getStartInclusiveInSec() {
    return startSecInclusive;
  }

  /**
   * @return end time in seconds
   */
  public long getEndExclusiveInSec() {
    return endSecExclusive;
  }

  /**
   * @return start time in milliseconds
   */
  public long getStartInclusiveInMs() {
    return startSecInclusive * 1000;
  }

  /**
   * @return end time in milliseconds
   */
  public long getEndExclusiveInMs() {
    return endSecExclusive * 1000;
  }

  /**
   * Decide whether a given date is included in this TimeInteval
   * 
   * @param timeInSec
   *          date in seconds
   * @return
   */
  public boolean include(long timeInSec) {
    return timeInSec >= startSecInclusive && timeInSec < endSecExclusive;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + (int) (endSecExclusive ^ (endSecExclusive >>> 32));
    result = 31 * result + (int) (startSecInclusive ^ (startSecInclusive >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;
    TimeRange other = (TimeRange) obj;
    return (endSecExclusive == other.endSecExclusive) && (startSecInclusive == other.startSecInclusive);
  }

  @Override
  public String toString() {
    return "[" + getStartInclusiveInSec() + "s, " + getEndExclusiveInSec() + "s )";
  }
}
