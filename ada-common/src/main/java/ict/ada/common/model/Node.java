package ict.ada.common.model;

import ict.ada.common.util.Hex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A Node in graph.<br>
 * This class is NOT thread-safe.
 */
public class Node {
  /**
   * Node id size in bytes<br>
   * Node id: |--Node type: 2Bytes--|---md5:16Bytes---|
   */
  public static final int NODEID_SIZE = 16 + NodeType.NODETYPE_BYTES_SIZE;

  private String name;
  List<String> snames;
  private List<String> additionals;
  private NodeType type;
  private byte[] id;

  private List<NodeAttribute> attributes;
  private List<WdeRef> wdeRefs;
  
  private List<String> ents;
  public Node(NodeType type, String name) {
    checkNameAndType(type, name);
    this.name = name;
    this.type = type;
  }

  public Node(byte[] id) {
    checkNodeId(id);
    this.id = id;
  }

  public Node setNameAndType(String name, NodeType type) {
    checkNameAndType(type, name);
    this.name = name;
    this.type = type;
    return this;
  }

  /**
   * Set name of this node. If type field is not available,an Exception will be thrown.
   */
  public Node setName(String name) {
	  getType();
    checkNameAndType(this.type, name);
    this.name = name;
    return this;
  }

  /**
   * @return never return null
   */
  public List<String> getSnames() {
    if (snames == null) {
      return Collections.emptyList();
    } else {
      return snames;
    }
  }
 
  public List<String> getAdditionals(){
	  if(additionals == null)
		  return Collections.emptyList();
	  else return additionals;
  }
  
  public List<String> getEnts(){
	  if(this.ents == null)
		  return Collections.emptyList();
	  else return ents;
  }
  
  // /**
  // * @param snames the snames to set
  // */
  // public void setSnames(List<String> snames) {
  // this.snames = snames;
  // }

  private void checkNameAndType(NodeType type, String name) {
    if (type == null || type.isAggregateType()) {
      throw new IllegalArgumentException("type=" + type);
    }
    if (name == null) throw new NullPointerException("null name");
  }

  public Node setId(byte[] id) {
    checkNodeId(id);
    this.id = id;
    return this;
  }

  public static void checkNodeId(byte[] nodeId) {
    if (nodeId == null) throw new NullPointerException("null id");
    if (nodeId.length != NODEID_SIZE)
      throw new IllegalArgumentException("id length=" + nodeId.length);
  }

  public void addNodeAttribute(NodeAttribute attr) {
    if (attributes == null) {// lazy-initialization, because some Nodes do not have attributes
      attributes = new ArrayList<NodeAttribute>();
    }
    attributes.add(attr);
  }

  public void addEnts(String ent){
	  if(this.ents == null){
		  this.ents = new ArrayList<String>();
	  }
	  this.ents.add(ent);
  }
  
  public void addWdeRef(WdeRef ref) {
    if (wdeRefs == null) {
      wdeRefs = new ArrayList<WdeRef>();
    }
    wdeRefs.add(ref);
  }

  public void addWdeRefs(List<WdeRef> refs) {
    if (wdeRefs == null) {
      wdeRefs = new ArrayList<WdeRef>();
    }
    for (WdeRef ref : refs) {
      wdeRefs.add(ref);
    }
  }

  
  public void addSearchName(String sname) {
    if (snames == null) {
      snames = new ArrayList<String>();
    }
    snames.add(sname);
  }

  public void addAdditional(String additional){
	  if(additionals == null)
		  additionals = new ArrayList<String>();
	  additionals.add(additional);
  }
  // public void removeSearchName(String sname) {
  // if (snames == null) return;
  // if (snames.contains(sname)) {
  // snames.remove(sname);
  // }
  // }

  /**
   * @param snames the snames to set
   */
  public void setSnames(List<String> snames) {
    this.snames = snames;
  }

  public void setAdditionals(List<String> additionals){
	  this.additionals = additionals;
  }
  
  public void setEnts(List<String> ents){
	  this.ents = ents;
  }
  /**
   * Check essential info
   * 
   * @return true if the Node has type+name or id
   */
  public boolean validate() {
    return (name != null && type != null) || (id != null);
  }

  public String getName() {
    return name;
  }

  public byte[] getId() {
    return id;
  }

  /**
   * If type field is not available, we will try to extract type from id.<br>
   * If no type info is available, NULL will be returned.
   */
  public NodeType getType() {
    // if (type == null && id != null) {
    // type = NodeType.getType(id[0], id[1] );
    // }
    if (type == null) {
      type = Node.getType(id);
    }
    return type;
  }

  /**
   * Get NodeType from a node id
   * 
   * @param nodeid
   * @return null if nodeid is null or node type in nodeid is invalid
   */
  public static NodeType getType(byte[] nodeid) {
    if (nodeid == null) return null;
    return NodeType.getType(nodeid[0], nodeid[1]);
  }

  public List<NodeAttribute> getAttributes() {
    return attributes;
  }

  public List<WdeRef> getWdeRefs() {
    if (wdeRefs == null) return Collections.emptyList();
    else return wdeRefs;
  }

  /**
   * Test if two Nodes represent the same node, considering ONLY name,type and id fields.<br>
   * TODO: how to deal with (id,null,null) and (null,name,type)...
   * 
   * @param node
   * @return
   */
  public boolean representSameNode(Node node) {
    if (node == null) return false;
    if (id != null && node.getId() != null) {
      return Arrays.equals(id, node.getId());
    } else if (name != null && type != null) {
      return name.equals(node.getName()) && type.equals(node.getType());
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "Node{ id=" + (id == null ? "null" : Hex.encodeHex(id)) + " n=" + name + "}";
  }
}
