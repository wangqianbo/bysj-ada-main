package ict.ada.gdb.schema;



import ict.ada.common.model.NodeType;

public class NodeWdeRefsHTable {
  public static final String FAMILY = "i";// column family
  public static final String QUALIFIER = "d";// qualifier: node detail("d")

  /**
   * Table name generation
   * 
   * @param type
   * @return
   */
  public static String getName(NodeType type) {
    return GdbHTablePartitionPolicy.getNodeRelatedHTableName(GdbHTableType.NODE_WDEREFS,type.getChannel() );
  }
}
