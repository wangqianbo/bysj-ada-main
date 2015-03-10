package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Node;

public class GetNodeEntsBean {
  private String errorCode = "success";
  private Node node = null;
  
  public GetNodeEntsBean(ict.ada.common.model.Node node){
    if(node != null){
      this.node  = new Node(node,null);
    }
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public Node getNode() {
    return node;
  }

  public void setNode(Node node) {
    this.node = node;
  }
  
}
