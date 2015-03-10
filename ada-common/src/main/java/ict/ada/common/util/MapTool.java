package ict.ada.common.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Toolkits for Java Map
 * 
 * @author Hs <hshcoder@gmail.com>
 * 
 * @param <K>
 * @param <V>
 */
public class MapTool {

  /**
   * New a HashMap with a proper initialCapacity based on the estimated keySet size.
   * <p>
   * When the keySet is very LARGE, we can avoid the cost of HashMap resizing by giving it a proper
   * initialCapacity. No need to use this when the Map keySet won't be very LARGE.
   * 
   * @param estimatedKeySetSize
   *          how many Keys will be put into this HashMap
   * @return a HashMap instance with proper initial capacity
   */
  public static <K, V> Map<K, V> newHashMap(int estimatedKeySetSize) {
    if (estimatedKeySetSize > 500) {
      int initialCapacity = (int) Math.ceil((estimatedKeySetSize / 0.75)) + 1;// see HashMap source
                                                                              // for more
      return new HashMap<K, V>(initialCapacity);
    } else {
      return new HashMap<K, V>();
    }
  }

  /**
   * New a HashMap.
   * <p>
   * This method equals to " new HashMap<K, V>()", but can avoid long type information in <K, V>.
   * 
   * @return a HashMap
   */
  public static <K, V> Map<K, V> newHashMap() {
    return new HashMap<K, V>();
  }

  /**
   * The capacity for HashMap or HashSet if we want to store "estimeatedKeySetSize" elements in it.
   * 
   * @param estimeatedKeySetSize
   * @return
   */
  public static int capacityEstimate(int estimeatedKeySetSize) {
    return (int) Math.ceil((estimeatedKeySetSize / 0.75)) + 1;// see HashMap source for more
  }
}
