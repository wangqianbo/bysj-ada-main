package ict.ada.gdb.schema;

import ict.ada.common.model.NodeType;

public class NodeAttributeHTable {
  public static final String FAMILY = "i";// column family
  public static final String QUALIFIER = "a";// qualifier: node attribute("a")
  public static final byte[] DELIMITER ={ 0x00};

  /**
   * Table name generation
   */
  public static String getName(NodeType type) {
    return GdbHTablePartitionPolicy.getNodeRelatedHTableName(GdbHTableType.NODE_ATTR, type.getChannel());
  }
}
