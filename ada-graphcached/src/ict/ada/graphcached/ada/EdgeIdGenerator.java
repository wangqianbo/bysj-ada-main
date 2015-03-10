/**
 * 
 */
package ict.ada.graphcached.ada;

import ict.ada.common.model.Edge;

/**
 * @author forhappy
 *
 */
public class EdgeIdGenerator {
  public static byte[] generate(Edge edge) {
    return edge.getId();
  }
}
