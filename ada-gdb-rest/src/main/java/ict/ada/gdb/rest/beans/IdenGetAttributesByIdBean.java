package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Node;

import java.util.ArrayList;
import java.util.List;

public class IdenGetAttributesByIdBean {
  public IdenGetAttributesByIdBean() {
    super();
    nodeList = new ArrayList<Node>();
  }

  public void addNode(ict.ada.common.model.Node node) {
    nodeList.add(new Node(node, null));
  }

  private List<Node> nodeList;
  private String errorCode = "success";

  /**
   * @return the nodeList
   */
  public List<Node> getNodeList() {
    return nodeList;
  }

  /**
   * @param nodeList
   *          the nodeList to set
   */
  public void setNodeList(List<Node> nodeList) {
    this.nodeList = nodeList;
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
