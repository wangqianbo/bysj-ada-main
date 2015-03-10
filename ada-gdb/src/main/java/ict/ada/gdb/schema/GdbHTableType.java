package ict.ada.gdb.schema;

/**
 * Define HTables used in GDB.<br>
 * One type per HTable, and each HTable can have multiple partitions.
 * 
 */
public enum GdbHTableType {
  /*
   * WARN: Take care to change the attached Strings. They will be used in HBase HTable names.
   */
  // Node
  NODE_ID("nodeId"), //
  NODE_NAME("nodeName"), //
  NODE_WDEREFS("nodeWdeRefs"), //
  NODE_ATTR("nodeAttr"), //

  // Edge
  EDGE_ID("edgeId"), //
  EDGE_REL_WEIGHT_DETAIL("edgeRelWeightDetail"), //
  EDGE_REL_WEIGHT_SUM("edgeRelWeightSum"), //
  RELATION_WDEREFS("relationWdeRefs"), //
  
  // Others
  LOCATION_NODE_TASKS("locationNodeTasks"),
  
  RELATION_TYPE("relationType");
  private String contentTag;// content tag for a HTable, which can be used in table names

  private GdbHTableType(String tag) {
    this.contentTag = tag;
  }

  /**
   * Get the content tag for a HTable, e.g. nodeWdeRefs, nodeId, which is used in HTable names.
   * 
   * @return
   */
  public String getContentTag() {
    return this.contentTag;
  }

  /**
   * Decide whether a table type is node-related.
   * 
   * @param type
   * @return
   */
  public static boolean isNodeRelatedTable(GdbHTableType type) {
    return type == NODE_ID || type == NODE_NAME || type == NODE_WDEREFS || type == NODE_ATTR;
  }
}
