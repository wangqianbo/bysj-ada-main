package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class GetReleventNodesBean {
  private String errorCode = "success";
  private List<SimilarNode> results;

  public GetReleventNodesBean(int len) {
    results = new ArrayList<SimilarNode>(len);
  }

  public void addNode( ict.ada.common.model.Node bean, List<ict.ada.common.model.Node> elements, int score) {
    results.add(new SimilarNode(bean, elements, score));
  }

  public String getErrorCode() {
    return errorCode;
  }

  public List<SimilarNode> getResults() {
    return results;
  }

  public static class SimilarNode {
    private int score;
    private Node nodeInfo;
    private List<Element> elements;

    public SimilarNode(ict.ada.common.model.Node bean, List<ict.ada.common.model.Node> elements, int score) {
      this.nodeInfo = new Node(bean,null);
      this.elements = new ArrayList<Element>();
      for (ict.ada.common.model.Node node : elements)
        this.elements.add(new Element(node));
      this.score = score;
    }

    public int getScore() {
      return score;
    }

    
  

    public List<Element> getElements() {
      return elements;
    }

    @JsonProperty("node_info")
    public Node getNodeInfo() {
      return nodeInfo;
    }
    
    
    
  }

  public static class Element {
    private String name;
    private String type;
    private List<String> sname;

    public Element(String name, String type, List<String> sname) {
      this.name = name;
      this.type = type;
      this.sname = sname;
    }

    public Element(ict.ada.common.model.Node node) {
      this.name = node.getName();
      this.type = NodeTypeMapper.getAttributeName(node.getType().getAttribute());
      this.sname = node.getSnames();
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public List<String> getSname() {
      return sname;
    }

  }
}
