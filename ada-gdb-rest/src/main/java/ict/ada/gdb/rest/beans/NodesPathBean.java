package ict.ada.gdb.rest.beans;

import ict.ada.gdb.util.NodeIdConveter;

import java.util.ArrayList;

public class NodesPathBean {
  private String errorCode = "success";
  private int counts;
  private ArrayList<Path> paths;

  public static class Path {
    private int length;
    private ArrayList<Node> path;

    public Path(ict.ada.gdb.rest.common.Path path) {
      this.length = path.getNodeList().size();
      this.path = new ArrayList<Node>();
      for (ict.ada.common.model.Node node : path.getNodeList()) {
        this.path.add(new Node(node));
      }
    }

    /**
     * @return the length
     */
    public int getLength() {
      return length;
    }

    /**
     * @param length
     *          the length to set
     */
    public void setLength(int length) {
      this.length = length;
    }

    /**
     * @return the path
     */
    public ArrayList<Node> getPath() {
      return path;
    }

    /**
     * @param path
     *          the path to set
     */
    public void setPath(ArrayList<Node> path) {
      this.path = path;
    }

  }

  public static class Node {
    private String nodeId;

    public Node(ict.ada.common.model.Node node) {
      this.nodeId = NodeIdConveter.toString(node.getId());
    }

    /**
     * @return the nodeId
     */
    public String getNodeId() {
      return nodeId;
    }

    /**
     * @param nodeId
     *          the nodeId to set
     */
    public void setNodeId(String nodeId) {
      this.nodeId = nodeId;
    }
  }

  /**
   * @return the count
   */
  public int getCount() {
    return counts;
  }

  /**
   * @param count
   *          the count to set
   */
  public void setCounts(int counts) {
    this.counts = counts;
  }

  /**
   * @return the paths
   */
  public ArrayList<Path> getPaths() {
    return paths;
  }

  /**
   * @param paths
   *          the paths to set
   */
  public void setPaths(ArrayList<Path> paths) {
    this.paths = paths;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

}
