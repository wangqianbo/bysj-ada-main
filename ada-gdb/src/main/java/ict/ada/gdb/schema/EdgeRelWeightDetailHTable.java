package ict.ada.gdb.schema;

import java.util.Set;

import ict.ada.common.model.NodeType;

public class EdgeRelWeightDetailHTable {
  // Schema
  public static final byte[] FAMILY = "i".getBytes();

  /**
   * Table name generation without considering aggregate type
   * 
   * @param headType
   * @return
   */
  public static String getName(NodeType headType ) {
    return GdbHTablePartitionPolicy.getEdgeRelatedHTableNameWithoutAggType(
        GdbHTableType.EDGE_REL_WEIGHT_DETAIL, headType.getChannel());
  }

}
