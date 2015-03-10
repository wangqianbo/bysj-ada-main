package ict.ada.gdb.rest.beans;

import ict.ada.common.model.RelationGraph;
import ict.ada.gdb.rest.beans.model.Edge;
import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.util.NodeIdConveter;

import java.util.LinkedList;
import java.util.List;

public class GetNodeHierarchyByIdBean {
  private String addtion;
  private String errorCode = "success";
  private List<Node> nodeList = null;
  private List<Edge> edgeList = null;
  public GetNodeHierarchyByIdBean() {
    nodeList = new LinkedList<Node>();
    edgeList = new LinkedList<Edge>();
  }
  public void addRelationGraph(RelationGraph rel) {
    for (ict.ada.common.model.Node node : rel.getOuterNodes())
      nodeList.add(new Node(node, NodeIdConveter.toString(rel.getCenterNode().getId())));
    for (ict.ada.common.model.Edge edge : rel.getCenterEdges())
      edgeList.add(new Edge(edge));
  }
  public void addNodeList(Node node) {
    nodeList.add(node);
  }

  public Node genNode() {
    return new Node();
  }

  public Node genNode(ict.ada.common.model.Node node) {
    return new Node(node, null);
  }

  /**
   * @return the addtion
   */
  public String getAddtion() {
    return addtion;
  }

  /**
   * @param addtion
   *          the addtion to set
   */
  public void setAddtion(String addtion) {
    this.addtion = addtion;
  }

  /**
   * @return the errorCode
   */
  public String getErrorcode() {
    return errorCode;
  }

  /**
   * @param errorCode
   *          the errorCode to set
   */
  public void setErrorcode(String errorCode) {
    this.errorCode = errorCode;
  }

  /**
   * @return the nodeList
   */
  public List<Node> getNodeList() {
    return nodeList;
  }

  public List<Edge> getEdgeList() {
    return edgeList;
  }
  /**
   * @param nodeList
   *          the nodeList to set
   */
  public void setNodeList(List<Node> nodeList) {
    this.nodeList = nodeList;
  }

}
