package ict.ada.gdb.schema;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Channel;

import java.util.Set;

public class RelationTypeHTable {
  // Schema
  public static final byte[] FAMILY = "i".getBytes();
  public static final byte[] QUALIFIER = "i".getBytes();
  public static final String NAME="relationtype";
  /**
   * Table name generation without considering aggregate type
   * 
   * @param headType
   * @return
   */
  public static String getName(Channel channel) {
	  return GdbHTablePartitionPolicy.getEdgeRelatedHTableNameWithoutAggType(GdbHTableType.RELATION_TYPE,channel);
  }

}
