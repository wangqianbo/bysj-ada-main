package ict.ada.gdb.schema;

import ict.ada.common.model.NodeType;

public class NodeNameHTable {

  /*
   * Make ColumnFamily/Column names as short as possible
   */
  public static final byte[] FAMILY = "i".getBytes();// column family
  public static final byte[] QUALIFIER = "i".getBytes();// qualifier: node id("i")

  /**
   * Table name generation
   * 
   * @param type
   *          The type of a node.
   * @return The name of corresponding table in which a node's value resides.
   */
  public static String getName(NodeType type) {
    return GdbHTablePartitionPolicy.getNodeRelatedHTableName(GdbHTableType.NODE_NAME, type.getChannel());
  }

  public static void main(String[] args){
	  int a =001050;
	  System.out.println(Integer.parseInt("001050"));
	  System.out.println(NodeNameHTable.getName(NodeType.getType(a)));
  }
  
}
