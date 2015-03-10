package ict.ada.gdb.schema;


import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Channel;

public class NodeIdHTable {
  public static final String FAMILY = "i";// column family
  public static final String QUALIFIER = "v";// qualifier: node value("v")
  public static final String SQUALIFIER = "s";// search name qualifier: node value("s")
  public static final String AQUALIFIER = "a";// additional qualifier: node value("addl")
  public static final String EQUALIFIER = "e"; // ents qualifier : node value("ents")
  /**
   * Table name generation
   * 
   * @param type
   * @return
   */
  public static String getName(NodeType type) {
    return GdbHTablePartitionPolicy.getNodeRelatedHTableName(GdbHTableType.NODE_ID, type.getChannel());
  }
  public static String getName(Channel channel){

	    return GdbHTablePartitionPolicy.getNodeRelatedHTableName(GdbHTableType.NODE_ID, channel);
	  
  }
}
