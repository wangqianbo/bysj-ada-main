package ict.ada.gdb.rest.util;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.RelationGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 这个类的目的是graph搜索，在系统中，经常会有搜索一些点的直接关系，从而组成一个图 这个类应起到：1,组建一个图，２，算法作用。３，缓存作用。
 * 
 */
public class QueryGraph {
  private List<Node> centerNodes;
  private List<Edge> sortedEdgeList;
  private List<Node> nodeList;// 和edgeList保持顺序一致,可以说用于为点加上名称。

  public QueryGraph(List<RelationGraph> relGraphs) {
    int edgecount = 0;
    for (RelationGraph relGraph : relGraphs) {
      edgecount += relGraph.getCenterEdges().size();
    }
    centerNodes = new ArrayList<Node>(edgecount);
    sortedEdgeList = new ArrayList<Edge>(edgecount);
    nodeList = new ArrayList<Node>(relGraphs.size());
    for (RelationGraph relGraph : relGraphs) {
      centerNodes.add(relGraph.getCenterNode());
      sortedEdgeList.addAll(relGraph.getCenterEdges());
    }
    Collections.sort(sortedEdgeList, new Comparator<Edge>() {
      @Override
      public int compare(Edge e1, Edge e2) {
        int flag = e2.getEdgeWeight() - e1.getEdgeWeight();
        if (flag == 0) flag = Bytes.compareTo(e1.getId(), e2.getId());// in
        // ascending
        // order.
        return flag;
      }

    });

    for (Edge edge : sortedEdgeList)
      nodeList.add(edge.getTail());
  }

  public List<Node> getCenterNodes() {
    return centerNodes;
  }

  public List<Edge> getSortedEdgeList() {
    return sortedEdgeList;
  }

  public List<Node> getNodeList() {
    return nodeList;
  }

}
