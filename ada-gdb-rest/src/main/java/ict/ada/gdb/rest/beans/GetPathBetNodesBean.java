package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Edge;
import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.util.NodeIdConveter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hdfs.util.ByteArray;

public class GetPathBetNodesBean {

  private String errorCode = "success";
  private List<Node> nodeList = null;
  private List<Edge> edgeList = null;

  public GetPathBetNodesBean() {
    nodeList = new ArrayList<Node>();
    edgeList = new ArrayList<Edge>();

  }

  public void addEdge(ByteArray edgeId, ict.ada.common.model.Node endnode) {
    Edge edge = new Edge(edgeId);
    byte[] edge_id = edgeId.getBytes();
    byte[] from_id = Arrays.copyOfRange(edge_id, 0, edge_id.length / 2);
    Node node = new Node(endnode, NodeIdConveter.toString(from_id));
    this.nodeList.add(node);
    this.edgeList.add(edge);
  }

  public void addNode(ict.ada.common.model.Node addnode) {
    Node node = new Node(addnode, "");
    this.nodeList.add(node);
  }

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
}
