/**
 * 
 */
package ict.ada.graphcached.model;

import ict.ada.graphcached.util.Triplet;


public class CachedNode {
  private String nodeId;

  /**
   * @return the nodeId
   */
  public String getNodeId() {
    return nodeId;
  }

  /**
   * @param nodeId the nodeId to set
   */
  public void setNodeId(String nodeId) {
    this.nodeId = nodeId;
  }

  /**
   * @param nodeId
   */
  public CachedNode(String nodeId) {
    super();
    this.nodeId = nodeId;
  }
  
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof CachedNode))
      return false;

    @SuppressWarnings("unchecked")
    final CachedNode cachedNode = (CachedNode) o;

    if (nodeId != null ? !nodeId.equals(cachedNode.getNodeId()) : cachedNode.getNodeId() != null)
      return false;

    return true;
  }

  public int hashCode() {
    int result;
    result = (nodeId != null ? nodeId.hashCode() : 0);
    return result;
  }

  public String toString() {
    return nodeId;
  }
  
}
