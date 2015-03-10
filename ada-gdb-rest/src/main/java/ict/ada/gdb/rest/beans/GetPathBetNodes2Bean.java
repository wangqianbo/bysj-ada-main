package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.NodesPathBean.Path;
import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.util.NodeIdConveter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hdfs.util.ByteArray;

public class GetPathBetNodes2Bean {

  private String errorCode = "success";
  private List<Node> nodeList = null;
  private List<Path> pathList = null;
  private List<Edge> edgeList = null;;
  private int count;

  public GetPathBetNodes2Bean() {
    nodeList = new ArrayList<Node>();
    pathList = new ArrayList<Path>();
    edgeList = new ArrayList<Edge>();

  }

  public void addNodes(Collection<ict.ada.common.model.Node> collection) {
    for (ict.ada.common.model.Node addnode : collection)
      this.nodeList.add(new Node(addnode, null));
  }

  public void addEdges(Collection<ict.ada.common.model.Edge> collection) {
    for (ict.ada.common.model.Edge edge : collection)
      this.edgeList.add(new Edge(edge));
  }

  public void addPaths(ict.ada.gdb.rest.common.Path[] paths) {
    this.count = 0;
    for (ict.ada.gdb.rest.common.Path path : paths) {
      if (path == null) continue;
      this.count++;
      this.pathList.add(new Path(path));
    }

  }

  public static class Edge {
    private String id;
    private List<Relation> relations;

    public Edge(ict.ada.common.model.Edge edge) {
      this.relations = new ArrayList<Relation>();
      this.id = NodeIdConveter.toString(edge.getId());
      for (ict.ada.common.model.Relation relation : edge.getRelations())
        relations.add(new Relation(relation));
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public List<Relation> getRelations() {
      return relations;
    }

    public void setRelations(List<Relation> relations) {
      this.relations = relations;
    }

  }

  public static class Relation {
    private String type;
    private int weight;

    public Relation(ict.ada.common.model.Relation relation) {
      this.type = relation.getType().toString();
      this.weight = relation.getWeight();
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public int getWeight() {
      return weight;
    }

    public void setWeight(int weight) {
      this.weight = weight;
    }

  }

  /**
   * @return the nodeList
   */
  public List<Node> getNodeList() {
    return nodeList;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public List<Path> getPathList() {
    return pathList;
  }

  public void setPathList(List<Path> pathList) {
    this.pathList = pathList;
  }

  public List<Edge> getEdgeList() {
    return edgeList;
  }

  public void setEdgeList(List<Edge> edgeList) {
    this.edgeList = edgeList;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public void setNodeList(List<Node> nodeList) {
    this.nodeList = nodeList;
  }

  /**
   * @return the edgeList
   */

}
