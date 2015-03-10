package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Node;

import java.util.ArrayList;
import java.util.List;

public class GetGcommunityBean {
  private String errorCode = "success";
  private List<List<Node>> lists;
  private int community;

  public GetGcommunityBean(List<List<ict.ada.common.model.Node>> clusterNodes) {
    lists = new ArrayList<List<Node>>();
    community = clusterNodes.size();
    for (List<ict.ada.common.model.Node> nodelist : clusterNodes) {
      ArrayList<Node> nodeList = new ArrayList<Node>();
      for (ict.ada.common.model.Node node : nodelist) {
        nodeList.add(new Node(node, null));
      }
      lists.add(nodeList);
    }
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public List<List<Node>> getLists() {
    return lists;
  }

  public void setLists(List<List<Node>> lists) {
    this.lists = lists;
  }

  public int getCommunity() {
    return community;
  }

  public void setCommunity(int community) {
    this.community = community;
  }

}
