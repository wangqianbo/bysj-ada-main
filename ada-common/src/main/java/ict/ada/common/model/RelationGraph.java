package ict.ada.common.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * 以某个点为中心的关系图。 记录了中心结点，与中心结点相关的结点，中心点与相关点之间的边，以及相关点之间的边。
 */
public class RelationGraph {
  private final Node centerNode;// center Node
  private final Collection<Node> outerNodes;// outer Nodes that has relation with center Node
  private final Collection<Edge> centerEdges;// Edges from center Node to outer Nodes

  public RelationGraph(Node centerNode) {
    if (centerNode == null) throw new IllegalArgumentException("null centerNode");
    this.centerNode = centerNode;

    // outterNodes = new HashSet<Node>();
    // centerEdges = new HashSet<Edge>();
    outerNodes = new LinkedList<Node>();
    centerEdges = new LinkedList<Edge>();
    // 查询处理时，LinkedList的海量remove性能超过了HashSet，因为remove的顺序与链表中元素顺序相同，使得LinkedList的remove接近O(1)
  }

  /**
   * Add one Edge as a center Edge
   * 
   * @param edge
   */
  public void addCenterEdge(Edge edge) {
    if (edge == null) throw new NullPointerException("null Edge");
    if (!edge.getHead().representSameNode(centerNode))
      throw new IllegalArgumentException(
          "The Edge to add doesn't start with RelationGraph's center Node.");
    centerEdges.add(edge);
    outerNodes.add(edge.getTail());
  }

  /**
   * Add all Edge in edges
   * 
   * @param edges
   */
  public void addCenterEdges(Collection<Edge> edges) {
    if (edges != null) {
      for (Edge edge : edges) {
        addCenterEdge(edge);
      }
    }
  }

  /**
   * 移除一条中心边，同时移除该中心边所连接的相关点
   * 
   * @param edge
   */
  public void removeCenterEdgeAndRelatedOuterNode(Edge edge) {
    if (edge == null) throw new NullPointerException("null edge");
    // The Edge may not be in centerEdges
    if (true == centerEdges.remove(edge)) {
      outerNodes.remove(edge.getTail());
    }
  }

  public Node getCenterNode() {
    return centerNode;
  }

  public Collection<Node> getOuterNodes() {
    return outerNodes;
  }

  public Collection<Edge> getCenterEdges() {
    return centerEdges;
  }

  public String toDetailedString() {
    StringBuilder sb = new StringBuilder();
    sb.append("=====RelationGraph=====\n");
    sb.append("Center Node: " + centerNode.toString() + "\n");
    sb.append("Center Edges: size=" + centerEdges.size() + "\n");
    for (Edge edge : centerEdges) {
      sb.append("\t" + edge.toString() + "\n");
    }
    sb.append("=======================\n");
    return sb.toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("=====RelationGraph=====\n");
    sb.append("Center Node: " + centerNode.toString() + "\n");
    sb.append("Center Edges: size=" + centerEdges.size() + "\n");
    sb.append("=======================\n");
    return sb.toString();
  }

  /**
   * 返回关系图中包含的中心边的一个列表，列表中的边按照边上weights的降序排列
   */
  public List<Edge> getCenterEdgesInDescCountOrder() {
    List<Edge> sortedEdges = new ArrayList<Edge>(centerEdges);
    Collections.sort(sortedEdges, new Comparator<Edge>() {
      @Override
      public int compare(Edge e1, Edge e2) {
        // in descending order
        return -(e1.getEdgeWeight() - e2.getEdgeWeight());
      }
    });
    return sortedEdges;
  }
}
