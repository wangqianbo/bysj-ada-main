package ict.ada.gdb.rest.beans.model;

import ict.ada.common.model.Relation;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.util.NodeIdConveter;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.annotate.JsonSerialize;

public class Node {

  private String id;
  private String name;
  private List<String> sname;
  private String type;
  private String channel;
  private String parentId;
  private List<String> ents;
  
  private List<EdgeInfo> infos;

  public Node() {
    super();
    sname = new ArrayList<String>();
  }

  /**
   * @param id
   * @param name
   * @param type
   * @param weight
   * @param parentId
   */
  public Node(String id, String name, List<String> sname, String channel, String type,
      String parentId) {
    super();
    this.id = id;
    this.name = name;
    this.channel = channel;
    this.type = type;
    this.parentId = parentId;
    this.sname = sname;
  }

  public Node(ict.ada.common.model.Node node, String parentId) {
    this.id = NodeIdConveter.toString(node.getId());
    this.name = node.getName();
    this.channel = NodeTypeMapper.getChannelName(node.getType().getChannel());
    this.type = NodeTypeMapper.getAttributeName(node.getType().getAttribute());
    
    this.parentId = parentId;
    if(node.getSnames() == null || node.getSnames().size() == 0){
      this.sname = null;
    }else{
      this.sname = node.getSnames();
    }
    if(node.getEnts() == null || node.getEnts().size() == 0){
      this.ents = null;
    }else{
      this.ents = node.getEnts();
    }
  }

  
  public void addSname(String sname) {
    this.sname.add(sname);
  }

  public void addEdgeInfo(ict.ada.common.model.Edge edge) {
    infos = new ArrayList<EdgeInfo>();
    for (Relation relation : edge.getRelations())
      infos.add(new EdgeInfo(relation));
  }
   

  /**
   * @return the id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id
   *          the id to set
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the sname
   */
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<String> getSname() {
    return sname;
  }

  public String getChannel() {
    return channel;
  }

  /**
   * @param sname
   *          the sname to set
   */

  public void setSname(List<String> sname) {
    this.sname = sname;
  }

  /**
   * @param name
   *          the name to set
   */
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the type
   */
  public String getType() {
    return type;
  }

  /**
   * @param type
   *          the type to set
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * @return the parentId
   */

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public String getParentId() {
    return parentId;
  }

  /**
   * @param parentId
   *          the parentId to set
   */
  public void setParentId(String parentId) {
    this.parentId = parentId;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<EdgeInfo> getInfos() {
    return infos;
  }
  
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<String> getEnts() {
    return ents;
  }

}
