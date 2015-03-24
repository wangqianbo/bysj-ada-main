package ict.ada.gdb.schema;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Channel;

import java.util.Set;

public class EdgeMemTable {
  // Schema
  public static final byte[] FAMILY = "i".getBytes();
  
  /**
   * Table name generation without considering aggregate type
   * 
   * @param headType
   * @return
   */
  public static String getName(NodeType headType) {
    return GdbHTablePartitionPolicy.getEdgeRelatedHTableNameWithoutAggType(GdbHTableType.EDGE_MEM,
        headType.getChannel());
  }
  public static String getName(Channel channel) {
	    return GdbHTablePartitionPolicy.getEdgeRelatedHTableNameWithoutAggType(GdbHTableType.EDGE_MEM,
	    		channel);
	  }
}
