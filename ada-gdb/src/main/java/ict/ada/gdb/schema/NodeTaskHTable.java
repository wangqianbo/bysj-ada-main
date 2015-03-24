package ict.ada.gdb.schema;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Channel;

public class NodeTaskHTable {
	  public static final byte[] FAMILY = "i".getBytes();// column family
	  /**
	   * Table name generation
	   * 
	   * @param type
	   * @return
	   */
	  public static String getName(NodeType type) {
	    return GdbHTablePartitionPolicy.getNodeRelatedHTableName(GdbHTableType.NODE_TASK, type.getChannel());
	  }
	  public static String getName(Channel channel){
		    return GdbHTablePartitionPolicy.getNodeRelatedHTableName(GdbHTableType.NODE_TASK, channel);
		  
	  }
}
