package ict.ada.gdb.rest.beans;

import java.util.ArrayList;
import java.util.List;

import org.javatuples.Triplet;

import cn.golaxy.yqpt2.dtsearch2.client.DTSearchDoc;

public class GetNodeNameFromInternalIndexBean {

  private String errorCode = "success";

  private List<Channel> results;

  public GetNodeNameFromInternalIndexBean() {
    results = new ArrayList<Channel>();
  }

  public void addChannel(String name, List<Triplet<String, Long, List<DTSearchDoc>>> results) {
    this.results.add(new Channel(name, results));
  }

  public String getErrorCode() {
    return errorCode;
  }

  public List<Channel> getResults() {
    return results;
  }

  public static class Node {
    private String name;
    private String sname;
    private String additional;

    public Node(String name, String sname, String additional) {
      this.name = name;
      this.sname = sname;
      this.additional = additional;
    }

    /**
     * @return the name
     */
    public String getName() {
      return name;
    }

    /**
     * @param name
     *          the name to set
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * @return the sname
     */
    public String getSname() {
      return sname;
    }

    /**
     * @param sname
     *          the sname to set
     */
    public void setSname(String sname) {
      this.sname = sname;
    }

    /**
     * @return the additional
     */
    public String getAdditional() {
      return additional;
    }

    /**
     * @param additional
     *          the additional to set
     */
    public void setAdditional(String additional) {
      this.additional = additional;
    }

    /**
     * 
     */
    public Node() {
      super();
    }

  }

  public static class Channel {
    private String channel;
    private List<Attribute> typeList;

    public Channel(String channel, List<Triplet<String, Long, List<DTSearchDoc>>> results) {
      this.channel = channel;
      typeList = new ArrayList<Attribute>(results.size());
      for (Triplet<String, Long, List<DTSearchDoc>> result : results)
        typeList.add(new Attribute(result.getValue0(), result.getValue1(), result.getValue2()));
    }

    public String getChannel() {
      return channel;
    }

    public List<Attribute> getTypeList() {
      return typeList;
    }

  }

  public static class Attribute {
    private String type;
    private List<Node> nodeList;
    private long count;

    public Attribute(String type, long count, List<DTSearchDoc> docs) {
      this.type = type;
      this.count = count;
      nodeList = new ArrayList<Node>(docs.size());
      for (DTSearchDoc doc : docs)
        nodeList.add(new Node(doc.fields.get(0), doc.fields.get(1), doc.fields.get(2)));
    }

    public String getType() {
      return type;
    }

    public List<Node> getNodeList() {
      return nodeList;
    }

    public long getCount() {
      return count;
    }

  }

}
