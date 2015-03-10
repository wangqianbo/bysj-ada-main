package ict.ada.gdb.rest.common;

import ict.ada.common.model.Node;

public class NodeDegree implements Comparable {
  private Node node;
  private int degree;

  public NodeDegree(Node node, int degree) {
    this.node = node;
    this.degree = degree;
  }

  @Override
  public int compareTo(Object o) {
    // TODO Auto-generated method stub
    return this.degree - ((NodeDegree) o).degree;
  }

  public boolean bigThan(NodeDegree nd) {
    return this.degree >= nd.degree;
  }

  /**
   * @return the node
   */
  public Node getNode() {
    return node;
  }

  /**
   * @param node
   *          the node to set
   */
  public void setNode(Node node) {
    this.node = node;
  }

  /**
   * @return the degree
   */
  public int getDegree() {
    return degree;
  }

  /**
   * @param degree
   *          the degree to set
   */
  public void setDegree(int degree) {
    this.degree = degree;
  }

}
