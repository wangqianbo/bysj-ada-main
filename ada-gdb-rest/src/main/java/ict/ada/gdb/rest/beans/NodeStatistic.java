package ict.ada.gdb.rest.beans;

import ict.ada.common.model.Edge;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.RelationType;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.util.NodeIdConveter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class NodeStatistic {
  private String nodeType;
  private int nodeTypeTotal;
  private List<Node> topNodeList;
  private List<Relation> relationStatistic;

  public NodeStatistic(NodeType nodeType, List<Edge> edges) {
    this.nodeType = NodeTypeMapper.getAttributeName(nodeType.getAttribute());
    this.nodeTypeTotal = edges.size();
    this.topNodeList = new ArrayList<Node>();
    this.relationStatistic = new ArrayList<Relation>();
    Map<RelationType, Integer> relationSMap = new HashMap<RelationType, Integer>();
    int count = 0;
    for (Edge edge : edges) {
      if (count < 10) this.topNodeList.add(new Node(edge.getTail(), edge.getEdgeWeight()));
      count++;
      for (ict.ada.common.model.Relation relation : edge.getRelations()) {
        RelationType relType = relation.getType();
        Integer weight = relationSMap.get(relType);
        if (weight == null) relationSMap.put(relType, 1);
        else relationSMap.put(relType, weight + 1);
      }
    }
    for (Entry<RelationType, Integer> relation : relationSMap.entrySet()) {
      this.relationStatistic.add(new Relation(relation.getKey().getStringForm(), relation
          .getValue()));
    }
  }

  public String getNodeType() {
    return nodeType;
  }

  public void setNodeType(String nodeType) {
    this.nodeType = nodeType;
  }

  public int getNodeTypeTotal() {
    return nodeTypeTotal;
  }

  public void setNodeTypeTotal(int nodeTypeTotal) {
    this.nodeTypeTotal = nodeTypeTotal;
  }

  public List<Node> getTopNodeList() {
    return topNodeList;
  }

  public void setTopNodeList(List<Node> topNodeList) {
    this.topNodeList = topNodeList;
  }

  public List<Relation> getRelationStatistic() {
    return relationStatistic;
  }

  public void setRelationStatistic(List<Relation> relationStatistic) {
    this.relationStatistic = relationStatistic;
  }

  public static class Node {
    private String id;
    private String name;
    private List<String> sname;
    private int nodeCount;

    public Node(ict.ada.common.model.Node node, int weight) {
      this.id = NodeIdConveter.toString(node.getId());
      this.name = node.getName();
      this.sname = node.getSnames();
      this.nodeCount = weight;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public List<String> getSname() {
      return sname;
    }

    public void setSname(List<String> sname) {
      this.sname = sname;
    }

    public int getNodeCount() {
      return nodeCount;
    }

    public void setNodeCount(int nodeCount) {
      this.nodeCount = nodeCount;
    }

  }

  public static class Relation {
    private String relationType;
    private int relationCount;

    public Relation(String relationType, int relationCount) {
      this.relationCount = relationCount;
      this.relationType = relationType;
    }

    public String getRelationType() {
      return relationType;
    }

    public void setRelationType(String relationType) {
      this.relationType = relationType;
    }

    public int getRelationCount() {
      return relationCount;
    }

    public void setRelationCount(int relationCount) {
      this.relationCount = relationCount;
    }

  }
}
