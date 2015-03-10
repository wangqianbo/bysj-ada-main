package ict.ada.gdb.rest.beans;

import ict.ada.common.model.RelationGraph;
import ict.ada.common.model.RelationType;
import ict.ada.common.util.Pair;
import ict.ada.gdb.rest.beans.model.Edge;
import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.util.NodeIdConveter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hdfs.util.ByteArray;
import org.codehaus.jackson.map.annotate.JsonSerialize;

public class GetRelationByNodeIdBean {
  public GetRelationByNodeIdBean() {
    nodeList = new ArrayList<Node>();
    edgeList = new ArrayList<Edge>();
    relationTypeList = new ArrayList<String>();
  }

  public void addRelationGraph(RelationGraph rel) {
    for (ict.ada.common.model.Node node : rel.getOuterNodes())
      nodeList.add(new Node(node, NodeIdConveter.toString(rel.getCenterNode().getId())));
    for (ict.ada.common.model.Edge edge : rel.getCenterEdges())
      edgeList.add(new Edge(edge));
  }

  public void addRelationGraph(RelationGraph rel, int start, int len) {
    for (ict.ada.common.model.Node node : rel.getOuterNodes())
      nodeList.add(new Node(node, NodeIdConveter.toString(rel.getCenterNode().getId())));
    for (ict.ada.common.model.Edge edge : rel.getCenterEdges())
      edgeList.add(new Edge(edge));
  }

  public void addRelationGraph(RelationGraph rel,
      HashMap<ByteArray, Pair<String, List<String>>> nodeIdNameMap) {
    for (ict.ada.common.model.Node node : rel.getOuterNodes()) {
      Pair<String, List<String>> nodeName = nodeIdNameMap.get(new ByteArray(node.getId()));
      nodeList.add(new Node(NodeIdConveter.toString(node.getId()), nodeName.getFirst(), nodeName
          .getSecond(), NodeTypeMapper.getChannelName(node.getType().getChannel()), NodeTypeMapper
          .getAttributeName(node.getType().getAttribute()), NodeIdConveter.toString(rel
          .getCenterNode().getId())));
    }
    for (ict.ada.common.model.Edge edge : rel.getCenterEdges())
      edgeList.add(new Edge(edge));
  }

  private String errorCode = "success";
  private List<Node> nodeList = null;
  private List<Edge> edgeList = null;
  private List<String> relationTypeList = null;
  private int total = 0;
  
  private List<NodeStatistic> nodeStatistic;

  /**
   * @return the nodeList
   */
  public List<Node> getNodeList() {
    return nodeList;
  }

  /**
   * @return the edgeList
   */
  public List<Edge> getEdgeList() {
    return edgeList;
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }

  /**
   * @param err
   *          the err to set
   */
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }
  
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<String> getRelationTypeList() {
    return relationTypeList;
  }

  public void setRelationTypeList(Collection<String> relationTypes) {
    if (relationTypes != null) {
      relationTypeList = new ArrayList<String>();
      for (String relationType : relationTypes)
        relationTypeList.add(relationType);
    }
  }
  
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<NodeStatistic> getNodeStatistic() {
    return nodeStatistic;
  }

  public void setNodeStatistic(List<NodeStatistic> nodeStatistic) {
    this.nodeStatistic = nodeStatistic;
  }

  /**
   * @return the err
   */
  public String getErrorCode() {
    return errorCode;
  }

  public GetRelationByNodeIdBean addNode(Node node) {
    if (nodeList == null) {
      nodeList = new ArrayList<Node>();
      nodeList.add(node);
    } else nodeList.add(node);
    return this;
  }

  public GetRelationByNodeIdBean addEdge(Edge edge) {
    if (edgeList == null) {
      edgeList = new ArrayList<Edge>();
      edgeList.add(edge);
    } else edgeList.add(edge);
    return this;
  }

}
