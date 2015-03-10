package ict.ada.gdb.rest.beans.model;

import java.util.Arrays;

import org.apache.hadoop.hdfs.util.ByteArray;

import ict.ada.gdb.util.NodeIdConveter;

public class Edge {

  private String id;
  private int weight;
  private String fromId;
  private String toId;

  /**
   * @param id
   * @param type
   * @param weight
   * @param fromId
   * @param toId
   */

  public Edge(ict.ada.common.model.Edge edge) {
    this.id = NodeIdConveter.toString(edge.getId());
    this.weight = edge.getEdgeWeight();
    int length = this.id.length();
    this.fromId = this.id.substring(0, length / 2);
    this.toId = this.id.substring(length / 2);
  }

  public Edge(ByteArray edgeId) {
    byte[] edge_id = edgeId.getBytes();
    byte[] from_id = Arrays.copyOfRange(edge_id, 0, edge_id.length / 2);
    byte[] to_id = Arrays.copyOfRange(edge_id, edge_id.length / 2, edge_id.length);
    this.id = NodeIdConveter.toString(edge_id);
    this.weight = 0;
    this.fromId = NodeIdConveter.toString(from_id);
    this.toId = NodeIdConveter.toString(to_id);

  }

  public Edge(String id, String fromId, String toId) {
    super();
    this.id = id;
    this.weight = 1;
    this.fromId = fromId;
    this.toId = toId;
  }

  public Edge(String id, int weight, String fromId, String toId) {
    super();
    this.id = id;
    this.weight = weight;
    this.fromId = fromId;
    this.toId = toId;
  }

  public Edge(byte[] fromId, byte[] endId) {
    this.fromId = NodeIdConveter.toString(fromId);
    this.toId = NodeIdConveter.toString(endId);
    this.id = this.fromId + this.toId;
    this.weight = 0;
  }

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id
   *          the id to set
   */
  public void setId(String id) {
    this.id = id;
  }

  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }

  /**
   * @return the fromId
   */
  public String getFromId() {
    return fromId;
  }

  /**
   * @param fromId
   *          the fromId to set
   */
  public void setFromId(String fromId) {
    this.fromId = fromId;
  }

  /**
   * @return the toId
   */
  public String getToId() {
    return toId;
  }

  /**
   * @param toId
   *          the toId to set
   */
  public void setToId(String toId) {
    this.toId = toId;
  }

}
