package ict.ada.gdb.rest.dao;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

public class GdbRestHTablePool {

  private static Log LOG = LogFactory.getLog(GdbRestHTablePool.class);
  private HTablePool pool;

  /**
   * Always use HBaseDAOFactory to get GdbHTablePool.
   * 
   * @throws ZooKeeperConnectionException
   */
  public GdbRestHTablePool(String zookeeper) throws ZooKeeperConnectionException {
    Configuration conf = new Configuration();
    conf.set("hbase.zookeeper.quorum", zookeeper);
    pool = new HTablePool(conf, 100);

  }

  public void close() throws IOException {
    pool.close();
  }

  public HTableInterface getEventDocRelTable() {
    return pool.getTable("ada_event_doc_rel");
  }

}
