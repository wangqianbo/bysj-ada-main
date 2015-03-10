package ict.ada.gdb.rest.beans;

import ict.ada.common.model.NodeType.Attribute;
import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class GetNodesRelRelativeNodesBean {
  private String errorCode = "success";
  private List<Channel> results;
  private int count;

  public GetNodesRelRelativeNodesBean(List<ict.ada.common.model.Node> result, int count) {
    HashMap<Attribute, List<Node>> map = new HashMap<Attribute, List<Node>>();
    for (ict.ada.common.model.Node node : result) {
      if (map.containsKey(node.getType().getAttribute())) map.get(node.getType().getAttribute())
          .add(new Node(node, null));
      else {
        List<Node> list = new ArrayList<Node>();
        list.add(new Node(node, null));
        map.put(node.getType().getAttribute(), list);
      }
      results = new ArrayList<Channel>(map.size());
      for (Entry<Attribute, List<Node>> e : map.entrySet())
        results.add(new Channel(NodeTypeMapper.getAttributeName(e.getKey()), e.getValue()));
    }
    this.count = count;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public List<Channel> getResults() {
    return results;
  }

  public int getCount() {
    return count;
  }

  public static class Channel {
    private String type;
    private List<Node> nodeList;

    public Channel(String type, List<Node> nodeList) {
      this.type = type;
      this.nodeList = nodeList;
    }

    public String getType() {
      return type;
    }

    public List<Node> getNodeList() {
      return nodeList;
    }

  }

}
