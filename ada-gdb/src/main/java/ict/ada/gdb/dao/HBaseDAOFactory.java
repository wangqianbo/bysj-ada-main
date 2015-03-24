package ict.ada.gdb.dao;

import ict.ada.gdb.common.AdaConfig;

import java.io.IOException;

public class HBaseDAOFactory {

  private HBaseDAOFactory() {// disable constructor
  }

  private static GdbHTablePool pool; // All DAO share a single HTablePool
  static {
    pool = new GdbHTablePool();
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          pool.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
  }

  public static HBaseEdgeDAO getHBaseEdgeDAO() {
    // return new HBaseEdgeDAO(pool);
    return getHBaseEdgeDAO(AdaConfig.ENABLE_GDB_CACHE);// 'true' to enable GDB Cache
  }

  public static HBaseEdgeDAO getHBaseEdgeDAO(boolean enableGdbCache) {
    if (!enableGdbCache) return new HBaseEdgeDAO(pool);
    else return new HBaseEdgeDaoWithCache(pool);
  }

  public static HBaseEdgeDaoWithCache getHBaseEdgeDaoWithCache() {
    return new HBaseEdgeDaoWithCache(pool);
  }

  public static HBaseNodeDAO getHBaseNodeDAO() {
    return new HBaseNodeDAO(pool);
  }

  public static HBaseWdeDAO getHBaseWdeDAO() {
    return new HBaseWdeDAO(pool);
  }

  public static HBaseAggregationDao getHBaseAggregationDao() {
    return new HBaseAggregationDao(pool);
 }
  public static ComputationScheduler getComputationScheduler(){
	  return new ComputationScheduler(pool);
  }
}
