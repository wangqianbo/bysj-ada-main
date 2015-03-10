package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Edge;
import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.util.NodeIdConveter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetTowLevelRelationGraphBean {

  private String errorCode = "success";
  private List<Node> nodeList = null;
  private List<Edge> edgeList = null;

  public GetTowLevelRelationGraphBean() {
    this.nodeList = new ArrayList<Node>();
    this.edgeList = new ArrayList<Edge>();

  }

  public void addEdge(ict.ada.common.model.Edge edge, ict.ada.common.model.Node endnode,
      String parentId) {
    Node node = new Node(endnode, parentId);
    this.nodeList.add(node);
    this.edgeList.add(new Edge(edge));
  }

  public void addNode(ict.ada.common.model.Node addnode) {
    Node node = new Node(addnode, "");
    this.nodeList.add(node);
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public List<Node> getNodeList() {
    return nodeList;
  }

  public void setNodeList(List<Node> nodeList) {
    this.nodeList = nodeList;
  }

  public List<Edge> getEdgeList() {
    return edgeList;
  }

  public void setEdgeList(List<Edge> edgeList) {
    this.edgeList = edgeList;
  }

}
