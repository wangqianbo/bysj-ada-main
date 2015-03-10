package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Node;

public class GetNodeNameByIdBean {
  private Node node;
  private String errorCode = "success";

  public GetNodeNameByIdBean(ict.ada.common.model.Node node) {
    this.node = new Node(node, null);
  }

  public Node getNode() {
    return node;
  }

  public String getErrorCode() {
    return errorCode;
  }

}
