package ict.ada.common.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 一条“路径”，包含一个或多个有顺序的Node，任意相邻的两点之间有一条Edge相连。
 */
public class Path {
  private List<Node> orderedNodes;// 路径中包含的有序的一组点。例如：a,b,c,d
  private List<Edge> orderedEdges;// 路径中点之间的边，顺序依照点的顺序。例如：a-b,b-c,c-d

  public Path() {
    orderedNodes = new ArrayList<Node>();
    orderedEdges = new ArrayList<Edge>();
  }

  /**
   * Append one Edge to this Path. The Edge must start with the Path's current tail.
   */
  public void appendEdge(Edge edge) {
    if (edge == null)
      throw new NullPointerException("null Edge");
    else if (!getEndNode().representSameNode(edge.getHead()))
      throw new IllegalArgumentException("The Edge to append does not start with Path's current tail.");

    if (orderedNodes.size() == 0) {
      orderedNodes.add(edge.getHead());
    }
    orderedNodes.add(edge.getTail());
    orderedEdges.add(edge);
  }

  /**
   * 返回路径的长度，即路径中边的数量。 如果路径仅含单个点或无点，长度为0。
   */
  public long getPathLength() {
    return orderedEdges.size();
  }

  public String toDetailedString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{PATH: length=" + getPathLength() + " ,nodes(size=" + orderedNodes.size() + "):"
        + Arrays.toString(orderedNodes.toArray()) + " ,Edges=" + Arrays.toString(orderedEdges.toArray())
        + "}");
    return sb.toString();
  }

  @Override
  public String toString() {
    String res = "{PATH: " + "edges count=" + orderedEdges.size() + " ,nodes(size=" + orderedNodes.size()
        + "):" + Arrays.toString(orderedNodes.toArray()) + "}";
    return res;
  }

  /**
   * 获得路径的起始点
   */
  public Node getStartNode() {
    if (orderedNodes.size() > 0)
      return orderedNodes.get(0);
    else
      return null;
  }

  /**
   * 获得路径的结束点
   */
  public Node getEndNode() {
    if (orderedNodes.size() > 0)
      return orderedNodes.get(orderedNodes.size() - 1);
    else
      return null;
  }

  /**
   * 返回一个路径中点的列表，列表中依次包含从起点到终点的各个点。
   */
  public List<Node> getNodeListInOrder() {
    return orderedNodes;
  }

  /**
   * 返回一个路径中边的列表，列表中依次包含从起点到终点的各个边。
   */
  public List<Edge> getEdgeListInOrder() {
    return orderedEdges;
  }

}
