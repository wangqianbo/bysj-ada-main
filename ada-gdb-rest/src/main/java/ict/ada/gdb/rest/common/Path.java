package ict.ada.gdb.rest.common;

import ict.ada.common.model.Node;
import java.util.ArrayList;
import java.util.List;

/**
 * 包括路径遍历算法
 */
public class Path {
  private double totalweight;
  private int length;
  private double weight;
  private ArrayList<Node> nodeList;

  public Path() {
    nodeList = new ArrayList<Node>();
    this.weight = 0;
    this.length = 0;
    this.totalweight = 0.0;
  }

  public Path(Node startNode) {
    nodeList = new ArrayList<Node>();
    nodeList.add(startNode);
    this.length = 1;
  }

  public Path(Node startNode, int weight) {
    this.totalweight = weight;
    nodeList = new ArrayList<Node>();
    nodeList.add(startNode);
    this.length = 1;
  }

  public Path(Path path) {
    this.nodeList = new ArrayList<Node>(path.getNodeList());
    this.totalweight = path.getTotalweight();
    this.length = path.getLength();
    // this.weight = path.totalweight / path.length;
  }

  public void addNode(Node node, int weight) {
    this.totalweight += weight;
    this.nodeList.add(node);
    this.length += 1;
  }

  public void addNode(Node node) {
    this.nodeList.add(node);
    this.length += 1;
  }

  public void addTotalWeight(int nodedegree) {
    this.totalweight += nodedegree;
  }

  public void calWeight() {
    if (nodeList.size() == 1) this.weight = -1;
    else this.weight = Math.log(this.totalweight) / (nodeList.size() - 1);
  }

  public Node removeEndNode() {
    // 异常处理
    Node end = null;
    if (length != 0) {
      end = nodeList.get(length - 1);
      nodeList.remove(length - 1);
      length--;
    }
    return end;
  }

  public void minusTotalWeight(int weight) {
    this.totalweight -= weight;
  }

  public boolean bigThan(Path path) {
    return this.weight >= path.weight;
  }

  /**
   * @return the totalweight
   */
  public double getTotalweight() {
    return totalweight;
  }

  /**
   * @param totalweight
   *          the totalweight to set
   */
  public void setTotalweight(double totalweight) {
    this.totalweight = totalweight;
  }

  /**
   * @return the length
   */
  public int getLength() {
    return length;
  }

  /**
   * @param length
   *          the length to set
   */
  public void setLength(int length) {
    this.length = length;
  }

  /**
   * @return the nodeList
   */
  public List<Node> getNodeList() {
    return nodeList;
  }

  /**
   * @param nodeList
   *          the nodeList to set
   */
  public void setNodeList(ArrayList<Node> nodeList) {
    this.nodeList = nodeList;
  }

  /**
   * @return the weight
   */
  public double getWeight() {
    return weight;
  }

  public boolean addpath(Path path) {
    for (Node node : path.getNodeList()) {
      if (this.nodeList.contains(node)) return false;
      else this.nodeList.add(node);
    }
    return true;
  }

  /**
   * @param weight
   *          the weight to set
   */
  public void setWeight(double weight) {
    this.weight = weight;
  }

  public void removeFirst() {
    this.nodeList.remove(0);
    this.length--;
  }
}
