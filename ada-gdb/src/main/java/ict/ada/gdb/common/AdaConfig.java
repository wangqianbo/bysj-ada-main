package ict.ada.gdb.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration parameters for ADA<br>
 * TODO redesign
 */
public class AdaConfig {

  // // Hadoop
  // public static final String HADOOP_JOB_TRACKER;
  // public static final String HADOOP_QUEUENAME;
  // public static final String HADOOP_NAMENODE;
  // public static final String ADA_LIB_PATH_URI;// ada dependencies in hdfs for MR jobs
  // public static final String ADA_TMP_PATH_URI;// temp folder for ada in hdfs
  //
  // // Oozie
  // public static final String OOZIE_SERVER_URI;
  // public static final String OOZIE_WORKFLOW_PATH_URI;
  // public static final String OOZIE_USER_NAME;
  //
  // // Neo4j
  // public static final String NEO4J_THRIFT_SERVER_IP;

  /*
   * Search System
   */
  /** Search Index Server Address (with port) */
  public static final String INDEX_SERVER_ADDR;
  /** Multi-thread index server facade address(with port) */
  // public static final String MULTITHREAD_INDEX_SERVER_ADDR;

  public static final String MQ_SERVER_ADDR;
  public static String MQ_NAME;

  public static final String ROWCOUNT_CHANNELS;
  public static final String RELATIONTYPE_CHANNELS;
  public static final String CONNECTION_STRING;
  public static final int TIME_GRANULARITY;
  public static final  boolean GRAPH_NODEATTR_ACTION_TIMESTAMP;
  
  /** whether to use GDB Cache */
  public static final boolean ENABLE_GDB_CACHE;
  /**
   * GDB client will request 'entry address' for cache cluster members info.
   * Any cache server address(ip:port) in the cache cluster is OK
   */
  public static final String GDB_CACHE_ENTRY_ADDRESS;

  /*
   * Common
   */
  /** whether self loop(Edge with identical head and tail Nodes) is accepted in graph */
  public static final boolean GRAPH_ACCEPT_SELFLOOP;

  /** Property file name */
  public static final String PROP_FILE_NAME = "ada_config.properties";

  static {
    Properties config = new Properties();
    try {
      InputStream is = AdaConfig.class.getClassLoader().getResourceAsStream(PROP_FILE_NAME);
      if (is == null) {
        throw new RuntimeException("Can not find " + PROP_FILE_NAME + " in java classpath.");
      }
      config.load(is);
      // HADOOP_JOB_TRACKER = getAndCheckString(config, "ada.hadoop.jobtracker");
      // HADOOP_QUEUENAME = getAndCheckString(config, "ada.hadoop.queuename");
      // HADOOP_NAMENODE = getAndCheckString(config, "ada.hadoop.namenode");
      // ADA_LIB_PATH_URI = getAndCheckString(config, "ada.lib.path");
      // ADA_TMP_PATH_URI = getAndCheckString(config, "ada.tmp.path");
      // OOZIE_SERVER_URI = getAndCheckString(config, "ada.oozie.server.uri");
      // OOZIE_WORKFLOW_PATH_URI = getAndCheckString(config, "ada.oozie.workflow.app.path");
      // OOZIE_USER_NAME = getAndCheckString(config, "ada.oozie.user.name");
      // NEO4J_THRIFT_SERVER_IP = getAndCheckString(config, "ada.neo4j.thriftserver.ip");
      // MULTITHREAD_INDEX_SERVER_ADDR = getAndCheckString(config,
      // "ada.indexserver.multithread.address");

      INDEX_SERVER_ADDR = getAndCheckString(config, "ada.indexserver.address");
      GRAPH_ACCEPT_SELFLOOP = getAndCheckBoolean(config, "ada.graph.accept.selfloop");
      GRAPH_NODEATTR_ACTION_TIMESTAMP=getAndCheckBoolean(config,"ada.graph.node.attr.action.timestamp");
      
      MQ_SERVER_ADDR = getAndCheckString(config, "ada.mqserver.address");

      ROWCOUNT_CHANNELS = config.getProperty("ada.gdb.rowcounter");

      RELATIONTYPE_CHANNELS = config.getProperty("ada.gdb.relationtypecounter");

      CONNECTION_STRING = getAndCheckString(config, "ada.gdb.zookeeperserver");
      
      TIME_GRANULARITY=Integer.parseInt(config.getProperty("ada.gdb.time.granularity"));

      if (config.containsKey("ada.mqserver.mqname")) {
        MQ_NAME = getAndCheckString(config, "ada.mqserver.mqname");
      } else {
        MQ_NAME = "ada_search_mq";
      }

      // optional
      ENABLE_GDB_CACHE = getAndCheckBoolean(config, "ada.gdbcache.enable", false);
      GDB_CACHE_ENTRY_ADDRESS = getAndCheckString(config, "ada.gdbcache.entry.address", null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static boolean getAndCheckBoolean(Properties config, String key, boolean defaultValue)
      throws IOException {
    return Boolean.parseBoolean(getAndCheckString(config, key, "" + defaultValue));
  }

  private static boolean getAndCheckBoolean(Properties config, String key) throws IOException {
    return Boolean.parseBoolean(getAndCheckString(config, key));
  }

  private static String getAndCheckString(Properties config, String key, String defaultValue)
      throws IOException {
    String value = config.getProperty(key);
    if (value == null || value.trim().length() == 0) value = defaultValue;
    return value;
  }

  private static String getAndCheckString(Properties config, String key) throws IOException {
    String value = config.getProperty(key);
    if (value == null || value.trim().length() == 0)
      throw new IOException("Invalid configuration. key=" + key + " value=" + value);
    return value;
  }

  public static void main(String[] args) {
    // System.out.println(AdaConfig.HADOOP_JOB_TRACKER);
    // System.out.println(AdaConfig.NEO4J_THRIFT_SERVER_IP);
    System.out.println(AdaConfig.GRAPH_ACCEPT_SELFLOOP);
    System.out.println(AdaConfig.CONNECTION_STRING);
  }
}
