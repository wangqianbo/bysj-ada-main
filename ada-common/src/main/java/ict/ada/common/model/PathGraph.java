package ict.ada.common.model;

import ict.ada.common.util.ByteArray;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * The Graph formed by Paths between 2 nodes. <br>
 * This Graph can carry data on Edge, but can not carry data on Node (so you need to fetch each
 * Node's data separately if you need).
 * <p>
 * Internally, Graph is stored as adjacency list. <br>
 * It is guaranteed that one conceptual Node has only one Node object.
 */
public class PathGraph {

  private Node graphStart;// shared start node
  private Node graphEnd;// shared end node
  private Map<Node, Set<Node>> adjNodeList;// adjacency nodes list of this graph.

  private Map<Node, List<Edge>> adjEdgeList;

  // IMPORTANT:
  // Make sure one conceptual Node has only one Node object, so that Node's data(e.g. attrs) will
  // not be duplicated and enable us to use Node as Map key and Set content.
  private Map<ByteArray, Node> idNodeMap;// Node id ==> Node object

  public PathGraph(byte[] startId, byte[] endId) {
    if (startId == null || endId == null)
      throw new NullPointerException("startId=" + startId + "  endId=" + endId);
    this.adjNodeList = new HashMap<Node, Set<Node>>();
    this.idNodeMap = new HashMap<ByteArray, Node>();
    this.adjEdgeList = new HashMap<Node, List<Edge>>();
    this.graphStart = getNode(startId);
    this.graphEnd = getNode(endId);
    
    // in case of an empty PathGraph, initialize start and end nodes' adjList.
    adjNodeList.put(graphStart, new HashSet<Node>());
    adjNodeList.put(graphEnd, new HashSet<Node>());
    adjEdgeList.put(graphStart, new ArrayList<Edge>());
    adjEdgeList.put(graphEnd, new ArrayList<Edge>());
  }

  /**
   * Get Node object of the given id.<br>
   */
  private Node getNode(byte[] nodeId) {
    ByteArray key = new ByteArray(nodeId);
    Node node = idNodeMap.get(key);
    if (node == null) {
      node = new Node(nodeId);
      idNodeMap.put(key, node);
    }
    return node;
  }

  /**
   * Add one Edge into this Graph.<br>
   * If the Edge already exists in this graph's topology, it will be ignored. Otherwise, it will be
   * added and extra data on Edge( such as relations,weight..) will also be preserved.
   * 
   * @param edge
   * @return true if Edge added, false if Edge ignored
   */
  public boolean addDirectedEdge(Edge edge) {
    checkEdge(edge);
    Node start = getNode(edge.getHead().getId());
    Node end = getNode(edge.getTail().getId());
    return internalAddDirectedEdge(start, end, edge);
  }

  /**
   * see comments in {@link #addDirectedEdge(Edge)}
   */
  public boolean addDirectedEdge(byte[] startId, byte[] endId) {
    Node start = getNode(startId);
    Node end = getNode(endId);
    Edge edge = new Edge(start, end);
    return internalAddDirectedEdge(start, end, edge);
  }

  private boolean internalAddDirectedEdge(Node start, Node end, Edge edge) {
    Set<Node> adjNodes = adjNodeList.get(start);
    if (adjNodes == null) {
      adjNodes = new HashSet<Node>(); // use Set to avoid duplicate insert
      adjNodes.add(end);
      adjNodeList.put(start, adjNodes);
      putEdgeIntoAdjEdgeList(start, edge);
      return true;
    } else {
      if (adjNodes.contains(end)) {
        return false;// duplicate Edge, so the given Edge is ignored.
      } else {
        adjNodes.add(end);
        putEdgeIntoAdjEdgeList(start, edge);
        return true;
      }
    }
  }

  private void putEdgeIntoAdjEdgeList(Node key, Edge edge) {
    List<Edge> edges = adjEdgeList.get(key);
    if (edges == null) {
      edges = new ArrayList<Edge>();
      edges.add(edge);
      adjEdgeList.put(key, edges);
    } else {
      edges.add(edge);
    }
  }

  private void checkEdge(Edge edge) {
    if (edge == null) throw new NullPointerException("null Edge");
    if (edge.getHead().getId() == null || edge.getTail().getId() == null)
      throw new IllegalArgumentException("Edge head and tail Node MUST contain id.");

  }

  /**
   * Get all Node in this graph.
   * 
   * @return
   */
  public Collection<Node> getNodeList() {
    if (idNodeMap != null) {
      return idNodeMap.values();
    } else {
      return Collections.emptySet();
    }
  }

  /**
   * Get the statistics of this PathGraph.
   * 
   * @return
   */
  public PathGraphStatistics getGraphStatistics() {
    PathGraphStatistics stat = new PathGraphStatistics();
    stat.nodeNum = getNodeList().size();

    int maxD = -1, minD = Integer.MAX_VALUE, totalD = 0;
    for (Entry<Node, Set<Node>> entry : adjNodeList.entrySet()) {
      int degree = entry.getValue().size();
      totalD += degree;
      maxD = Math.max(maxD, degree);
      minD = Math.min(minD, degree);
    }
    stat.maxDegree = maxD;
    stat.minDegree = minD;
    stat.edgeNum = totalD / 2;
    stat.avgDegree = totalD * 1.0 / stat.nodeNum;
    return stat;
  }

  public static class PathGraphStatistics {
    private int nodeNum;
    private int edgeNum;
    private int maxDegree;
    private int minDegree;
    private double avgDegree;

    public int getNodeNum() {
      return nodeNum;
    }

    public int getEdgeNum() {
      return edgeNum;
    }

    public int getMaxDegree() {
      return maxDegree;
    }

    public int getMinDegree() {
      return minDegree;
    }

    public double getAvgDegree() {
      return avgDegree;
    }

    @Override
    public String toString() {
      return "[PathGraph Statistics] NodeNumber=" + nodeNum + "\tEdgeNumber=" + edgeNum
          + "\tMaxDegree=" + maxDegree + "\tMinDegree=" + minDegree + "\tAvgDegree=" + avgDegree;
    }
  }

  /**
   * Get each Node's adjacent Edges.<br>
   * In every <Node, List<Edge>> entry, it's guaranteed that each Edge's start node is equal to node
   * object in the entry key.
   * 
   * @return
   */
  public Map<Node, List<Edge>> getAdjacentEdgeList() {
    return adjEdgeList;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("========PathGraph=======\n");
    sb.append("Start Node=" + graphStart + "\n");
    sb.append("End node=" + graphEnd + "\n");
    sb.append("Total Nodes=" + getNodeList().size() + "\n");
    for (Entry<Node, List<Edge>> e : adjEdgeList.entrySet()) {
      sb.append(e.getKey().getId() + " " + e.getValue().size() + "\n");
    }
    sb.append("========================\n");
    return sb.toString();
  }

  /**
   * start node
   */
  public Node getGraphStart() {
    return graphStart;
  }

  /**
   * end node
   */
  public Node getGraphEnd() {
    return graphEnd;
  }

  
  /**
   * @return the adjNodeList
   */
  public Map<Node, Set<Node>> getAdjNodeList() {
    return adjNodeList;
  }

  /**
   * @return the idNodeMap
   */
  public Map<ByteArray, Node> getIdNodeMap() {
    return idNodeMap;
  }

  public static void main(String[] args) {
    PathGraph graph = new PathGraph("331234567890abcdef".getBytes(),
        "331234567890abcdeg".getBytes());
    graph.addDirectedEdge(new Edge(new Node("331234567890abcdeg".getBytes()), new Node(
        "331234567890abcdef".getBytes())));
    graph.addDirectedEdge(new Edge(new Node("331234567890abcdek".getBytes()), new Node(
        "331234567890abcdez".getBytes())));

    System.out.println(graph);

  }
}
