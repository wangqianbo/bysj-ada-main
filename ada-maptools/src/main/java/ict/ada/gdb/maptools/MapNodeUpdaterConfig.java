package ict.ada.gdb.maptools;

import ict.ada.common.model.NodeType;
import ict.ada.gdb.schema.LocationNodeTasksHTable;
import ict.ada.gdb.schema.NodeAttributeHTable;
import ict.ada.gdb.schema.NodeIdHTable;
import ict.ada.gdb.schema.NodeNameHTable;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

public class MapNodeUpdaterConfig {
  static MapNodeConfiguration mapNodeConf = new MapNodeConfiguration("conf/ada-mapnode.properties");
  static HBaseConfiguration conf = null;
  static HTablePool pool = null;
  static {
    conf = new HBaseConfiguration();
    pool = new HTablePool(conf, 16);
    
  }
  static public HTableInterface getNodeTaskTable() {
    return pool.getTable(LocationNodeTasksHTable.getName());
  }
  
  static public HTableInterface getNodeAttrTable() {
    return pool.getTable(NodeAttributeHTable.getName(NodeType.PLACE_ATTR));
  }
  
  static public HTableInterface getNodeIdTable() {
    return pool.getTable(NodeIdHTable.getName(NodeType.PLACE_ATTR));
  }
  
  static public HTableInterface getNodeNameTable() {
    return pool.getTable(NodeNameHTable.getName(NodeType.PLACE_ATTR));
  }
}
