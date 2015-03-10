package ict.ada.gdb.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.schema.CommunityPersonRelHTable;
import ict.ada.gdb.schema.EdgeIdHTable;
import ict.ada.gdb.schema.EdgeRelWeightDetailHTable;
import ict.ada.gdb.schema.EdgeRelWeightSumHTable;
import ict.ada.gdb.schema.EventDocRelHTable;
import ict.ada.gdb.schema.EventRelHTable;
import ict.ada.gdb.schema.LocationNodeTasksHTable;
import ict.ada.gdb.schema.NodeAttributeHTable;
import ict.ada.gdb.schema.NodeIdHTable;
import ict.ada.gdb.schema.NodeNameHTable;
import ict.ada.gdb.schema.NodeWdeRefsHTable;
import ict.ada.gdb.schema.RelationTypeHTable;
import ict.ada.gdb.schema.RelationWdeRefsHTable;
import ict.ada.gdb.schema.TableRowCountTable;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * An HTable pool in GDB, which also manages HTable names
 * 
 */
public class GdbHTablePool {
  private static Log LOG = LogFactory.getLog(GdbHTablePool.class);

  private HTablePool pool; // shared table pool

  /**
   * Separate conf file for GDB.
   * Its contents are the same as hbase-site.xml and will override fields in hbase-site.xml
   */
  private static final String GDB_HBASE_CONF_FILE_NAME = "gdb-hbase-site.xml";

  /**
   * Always use HBaseDAOFactory to get GdbHTablePool.
   */
  protected GdbHTablePool() {
    Configuration conf = HBaseConfiguration.create();
    // There may be multiple HBase instance in applications, so we need a separate conf for GDB.
    // Try to find gdb-hbase-site.xml in java classpath. If not found, GDB will fall back to use
    // configurations in hbase-site.xml
    conf.addResource(GDB_HBASE_CONF_FILE_NAME);
    // Tips: "quiet" field in Configuration is true by default, so no exception will be thrown if
    // any added resource file does not exist.

    pool = new HTablePool(conf, Integer.MAX_VALUE);
  }

  public void close() throws IOException {
    pool.close();
  }

  /*
   * Node Tables
   */
  public HTableInterface getNodeNameTable(NodeType type) {
    return pool.getTable(NodeNameHTable.getName(type));
  }

  public HTableInterface getNodeIdTable(NodeType type) {
    return pool.getTable(NodeIdHTable.getName(type));
  }

  public HTableInterface getNodeAttributeTable(NodeType type) {
    return pool.getTable(NodeAttributeHTable.getName(type));
  }

  public HTableInterface getNodeWdeRefsTable(NodeType type) {
    return pool.getTable(NodeWdeRefsHTable.getName(type));
  }

  public HTableInterface getLocationNodeTaskTable() {
    return pool.getTable(LocationNodeTasksHTable.getName());
  }

  /*
   * Edge Tables
   */
  public HTableInterface getRelationWdeRefsTable(NodeType headNodeType) {
    return pool.getTable(RelationWdeRefsHTable.getName(headNodeType));
  }

  public HTableInterface getEdgeIdTable(NodeType headNodeType) {
    return pool.getTable(EdgeIdHTable.getName(headNodeType));
  }

  public HTableInterface getEdgeRelWeightDetailTable(NodeType headNodeType) {
    return pool.getTable(EdgeRelWeightDetailHTable.getName(headNodeType));
  }

  public HTableInterface getEdgeRelWeightSumTable(NodeType headNodeType) {
    return pool.getTable(EdgeRelWeightSumHTable.getName(headNodeType));
  }

  /**
   * Non existent HTable names.<br>
   * ATTENTION: once a table name is added, we need a JVM restart to clear it.
   */
  private static final Set<String> nonexistentTableNames = new HashSet<String>();

  private List<HTableInterface> getTablesByNames(Set<String> names) {
    if (names.size() == 1) {
      return Collections.singletonList(pool.getTable(names.iterator().next()));
    } else {
      List<HTableInterface> tables = new ArrayList<HTableInterface>();
      for (String name : names) {
        if (nonexistentTableNames.contains(name)) continue;
        try {
          tables.add(pool.getTable(name));
        } catch (Exception e) {// Some channels may have not been used, so no HTable for them.
          Throwable rootException = ExceptionUtils.getRootCause(e);
          if (rootException instanceof TableNotFoundException) {
            LOG.warn(rootException.getMessage() + " HTable is not found. Will ignore it. ");
            nonexistentTableNames.add(rootException.getMessage());// will ignore it next time.
          } else throw new RuntimeException("Unable to get HTable: " + name, e);
        }
      }
      return tables;
    }
  }

  public HTableInterface getTableRowCountTable() {
    return pool.getTable(TableRowCountTable.getName());
  }

  public HTableInterface getRelationTypeTable(Channel channel) {
    return pool.getTable(RelationTypeHTable.getName(channel));
  }

  public HTableInterface getEventDocRelTable() {
    return pool.getTable(EventDocRelHTable.getName());
  }

  public HTableInterface getEventRelTable() {
    return pool.getTable(EventRelHTable.getName());
  }

  public HTableInterface getCommunityPersonRelTable() {
    return pool.getTable(CommunityPersonRelHTable.getName());
  }

  public boolean exist(String tableName) {// 这个方法有点不妥，
    try {
      pool.getTable(tableName);
    } catch (RuntimeException e) {
      return false;
    }
    return true;
  }
}
