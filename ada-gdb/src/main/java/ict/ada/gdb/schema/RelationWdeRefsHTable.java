package ict.ada.gdb.schema;

import java.util.Set;

import ict.ada.common.model.NodeType;

public class RelationWdeRefsHTable {

  // Schema
  public static final byte[] FAMILY = "i".getBytes();

  /**
   * Table name generation without considering aggregate type
   * 
   * @param headType
   * @param tailType
   *          TODO
   * @return
   */
  public static String getName(NodeType headType) {
    return GdbHTablePartitionPolicy.getEdgeRelatedHTableNameWithoutAggType(
        GdbHTableType.RELATION_WDEREFS, headType.getChannel());
  }
 
}
