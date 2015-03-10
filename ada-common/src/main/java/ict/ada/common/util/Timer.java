package ict.ada.common.util;

/**
 * Tools for time tracking.
 */
public class Timer {

  /**
   * Nanoseconds since a given start time(in nanoseconds)
   * 
   * @param startInNS
   *          a long from System.nanoTime() or Timer.now()
   * @return
   */
  public static long nsSince(long startInNS) {
    return System.nanoTime() - startInNS;
  }

  /**
   * Milliseconds since a given start time(in nanoseconds)
   * 
   * @param startInNS
   *          a long from System.nanoTime() or Timer.now()
   * @return
   */
  public static double msSince(long startInNS) {
    return (nsSince(startInNS) * 1.0 / 1000000);
  }

  /**
   * Seconds since a given start time(in nanoseconds)
   * 
   * @param startInNS
   *          a long from System.nanoTime() or Timer.now()
   * @return
   */

  public static double secSince(long startInNS) {
    return msSince(startInNS) / 1000;
  }

  /**
   * Equivalence of System.nanoTime();
   */
  public static long now() {
    return System.nanoTime();
  }
}
