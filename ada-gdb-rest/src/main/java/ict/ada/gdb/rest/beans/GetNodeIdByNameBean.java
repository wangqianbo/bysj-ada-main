package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Node;

public class GetNodeIdByNameBean {

  private Node node = null;
  private String errorCode = "success";

  /**
   * @return the node
   */
  public Node getNode() {
    return node;
  }

  /**
   * @param node
   *          the node to set
   */
  public void setNode(Node node) {
    this.node = node;
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
