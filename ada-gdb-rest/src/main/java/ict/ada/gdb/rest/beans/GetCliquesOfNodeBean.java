package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Edge;
import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.util.NodeIdConveter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetCliquesOfNodeBean {

  private String errorCode = "success";
  private List<Node> nodeList = null;
  private List<Edge> edgeList = null;
  private Statistics statistics = null;

  public GetCliquesOfNodeBean() {
    this.nodeList = new ArrayList<Node>();
    this.edgeList = new ArrayList<Edge>();

  }

  public void addEdge(byte[] startnodeId, ict.ada.common.model.Node endnode) {
    Edge edge = new Edge(startnodeId, endnode.getId());
    Node node = new Node(endnode, NodeIdConveter.toString(startnodeId));
    this.nodeList.add(node);
    this.edgeList.add(edge);
  }

  public void addNode(ict.ada.common.model.Node addnode) {
    Node node = new Node(addnode, "");
    this.nodeList.add(node);
  }

  public void addStatistics(Map<Integer, Integer> cliqueSta, double density, int totalCliques,
      int vertex, int edge) {
    statistics = new Statistics();
    statistics.setCliqueSta(cliqueSta);
    statistics.setDensity(density);
    statistics.setTotalCliques(totalCliques);
    statistics.setVertex(vertex);
    statistics.setEdge(edge);
  }

  public class Statistics {
    private int vertex;
    private int edge;
    private double density = 0.0;
    private int totalCliques;
    private Map<Integer, Integer> cliqueSta;

    /**
     * @return the cliqueSta
     */
    public Map<Integer, Integer> getCliqueSta() {
      return cliqueSta;
    }

    /**
     * @param cliqueSta
     *          the cliqueSta to set
     */
    public void setCliqueSta(Map<Integer, Integer> cliqueSta) {
      this.cliqueSta = cliqueSta;
    }

    /**
     * @return the density
     */
    public double getDensity() {
      return density;
    }

    /**
     * @param density
     *          the density to set
     */
    public void setDensity(double density) {
      this.density = density;
    }

    /**
     * @return the totalCliques
     */
    public int getTotalCliques() {
      return totalCliques;
    }

    /**
     * @param totalCliques
     *          the totalCliques to set
     */
    public void setTotalCliques(int totalCliques) {
      this.totalCliques = totalCliques;
    }

    /**
     * @return the vertex
     */
    public int getVertex() {
      return vertex;
    }

    /**
     * @param vertex
     *          the vertex to set
     */
    public void setVertex(int vertex) {
      this.vertex = vertex;
    }

    /**
     * @return the edge
     */
    public int getEdge() {
      return edge;
    }

    /**
     * @param edge
     *          the edge to set
     */
    public void setEdge(int edge) {
      this.edge = edge;
    }

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

  /**
   * @param nodeList
   *          the nodeList to set
   */
  public void setNodeList(List<Node> nodeList) {
    this.nodeList = nodeList;
  }

  /**
   * @param edgeList
   *          the edgeList to set
   */
  public void setEdgeList(List<Edge> edgeList) {
    this.edgeList = edgeList;
  }

  /**
   * @return the statistics
   */
  public Statistics getStatistics() {
    return statistics;
  }

  /**
   * @param statistics
   *          the statistics to set
   */
  public void setStatistics(Statistics statistics) {
    this.statistics = statistics;
  }

}
