package ict.ada.gdb.rest.beans;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import ict.ada.gdb.rest.beans.model.Edge;
import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.util.NodeIdConveter;

public class GetNodeRelNodeClusterByIdBean {
  private List<List<Node>> clusters = null;
  private List<Edge> edgeList = null;
  private String errorCode = "success";

  /**
   * @return the clusters
   */
  public GetNodeRelNodeClusterByIdBean() {
    clusters = new ArrayList<List<Node>>();
    edgeList = new ArrayList<Edge>();
  }

  public void genClusters(List<List<ict.ada.common.model.Node>> clusters, String patentId) {
    for (List<ict.ada.common.model.Node> cluster : clusters) {
      ArrayList<Node> clusterthis = new ArrayList<Node>();
      for (ict.ada.common.model.Node node : cluster)
        clusterthis.add(new Node(node, patentId));
      this.clusters.add(clusterthis);
    }
  }

  public void genEdgeList(Collection<ict.ada.common.model.Edge> edges) {
    for (ict.ada.common.model.Edge edge : edges) {
      edgeList.add(new Edge(edge));
    }
  }

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
   * @return the edgeList
   */
  public List<Edge> getEdgeList() {
    return edgeList;
  }

  /**
   * @param edgeList
   *          the edgeList to set
   */
  public void setEdgeList(List<Edge> edgeList) {
    this.edgeList = edgeList;
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
