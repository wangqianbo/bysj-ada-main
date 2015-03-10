package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.rest.beans.model.Edge;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class GetNodeClusterCBean {
  private List<List<Node>> clusters;
  private List<Edge> edgeList = null;
  private String errorCode = "success";

  public GetNodeClusterCBean() {
    clusters = new ArrayList<List<Node>>();
    edgeList = new ArrayList<Edge>();
  }

  public void addCluster(List<Node> cluster) {
    this.clusters.add(cluster);
  }

  public void addEdges(Map<String, Map<String, Double>> graph) {
    for (Entry<String, Map<String, Double>> adj : graph.entrySet()) {
      for (Entry<String, Double> e : adj.getValue().entrySet())
        edgeList.add(new Edge(adj.getKey() + e.getKey(), (int) e.getValue().intValue(), adj
            .getKey(), e.getKey()));
    }
  }

  public List<Edge> getEdgeList() {
    return edgeList;
  }

  public void setEdgeList(List<Edge> edgeList) {
    this.edgeList = edgeList;
  }

  /**
   * @return the clusters
   */
  public List<List<Node>> getClusters() {
    return clusters;
  }

  /**
   * @param clusters
   *          the clusters to set
   */
  public void setClusters(List<List<Node>> clusters) {
    this.clusters = clusters;
  }

  /**
   * @return the errorCode
   */
  public String getErrorCode() {
    return errorCode;
  }

  /**
   * @param errorCode
   *          the errorCode to set
   */
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

}
