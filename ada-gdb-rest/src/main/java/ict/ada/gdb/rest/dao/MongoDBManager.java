package ict.ada.gdb.rest.dao;

import ict.ada.gdb.rest.services.InternalServiceResources;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.commons.digester.plugins.strategies.LoaderSetProperties;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.ServerAddress;

public class MongoDBManager {

  private static Properties databaseProperty = getDBProperty();
  private static String ip = databaseProperty.getProperty("ada_identity_ip");
  private static int port = Integer.parseInt(databaseProperty.getProperty("ada_identity_port"));
  private static String dbName = databaseProperty.getProperty("ada_identity_dbName");
  private static int poolSize = Integer.valueOf(databaseProperty
      .getProperty("ada_identity_poolSize"));
  private static MongoClient mongoClient = null;
  private static DB db = null;
  static {
    System.setProperty("MONGO.POOLSIZE", String.valueOf(poolSize));
    if (mongoClient == null) {
      try {
        Builder builder = new MongoClientOptions.Builder();
        builder.autoConnectRetry(true);
        builder.connectionsPerHost(poolSize);
        if (port == 0) port = 27017;
        mongoClient = new MongoClient(new ServerAddress(ip, port), builder.build());
        db = mongoClient.getDB(dbName);
      } catch (UnknownHostException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  private static synchronized DB getDB() {
    if (db == null) {

    }
    return db;
  }

  /**
   * 通过本函数获取一个指定的collection
   * 
   * @param collectionName
   * @return
   */
  public static synchronized DBCollection getDbCollection(String collectionName) {
    getDB();
    return db.getCollection(collectionName);
  }

  /**
   * 关闭连接
   */
  public static synchronized void close() {
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
      db = null;
    }
  }

  /**
   * 获取配置文件属性
   * 
   * @return
   */
  private static Properties getDBProperty() {
    return InternalServiceResources.getAdaGdbRestConf();
  }

  public static void main(String[] args) {
    MongoDBManager m = new MongoDBManager();
    DBCollection monitoredWeiboUserCol = MongoDBManager.getDbCollection("monitored_weibo_user");
    System.out.println(monitoredWeiboUserCol.count());
  }
}
