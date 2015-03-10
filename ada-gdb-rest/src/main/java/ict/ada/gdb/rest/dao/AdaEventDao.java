package ict.ada.gdb.rest.dao;

import ict.ada.gdb.rest.dao.bean.AdaEventBean;
import ict.ada.gdb.rest.services.InternalServiceResources;
import ict.ada.gdb.rest.util.PojoMapper;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;

/**
 * 完全基于实现，应该基于接口！！！！
 * */
/**
 * @author wangqianbo
 * 
 */
public class AdaEventDao {
  private MongoClient mongoClient;
  private DB db;
  private String host = InternalServiceResources.getAdaGdbRestConf()
      .getProperty("ada_event_db_url");
  private String dbName = "ada_event";
  private String collection = "ada_event";
  private DBCollection coll;
  private static Cache<String, List<DBObject>> resultCache = CacheBuilder.newBuilder()
      .concurrencyLevel(4).maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES).build();

  public AdaEventDao() {
    try {
      mongoClient = new MongoClient(host);
    } catch (UnknownHostException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    db = mongoClient.getDB(dbName);
    coll = db.getCollection(collection);
  }

  public List<AdaEventBean> getNodesByTags(Collection<String> tags) throws Exception {
    BasicDBObject query = new BasicDBObject("tags", new BasicDBObject("$in", tags));
    DBCursor cursor = coll.find(query);
    List<AdaEventBean> result = new ArrayList<AdaEventBean>();
    try {
      while (cursor.hasNext()) {
        DBObject eventDoc = cursor.next();
        AdaEventBean bean = getAdaEventBean(eventDoc);
        if (bean != null) result.add(bean);
      }
    } catch (Exception e) {
      throw new Exception(e);
    } finally {
      cursor.close();
    }
    return result;
  }

  public List<AdaEventBean> getNodesByTags(final List<String> tags, int start, int len,
      ArrayList<Integer> count) throws Exception {
    StringBuilder key = new StringBuilder("getNodesByTags");
    for (String tag : tags)
      key.append(tag);
    List<DBObject> results = resultCache.get(key.toString(), new Callable<List<DBObject>>() {
      @Override
      public List<DBObject> call() {
        return getNodesByTags1(tags);
      }
    });
    count.add(results.size());
    List<AdaEventBean> result = new ArrayList<AdaEventBean>();
    for (int i = start; i < start + len && i < results.size(); i++) {
      AdaEventBean bean = getAdaEventBean(results.get(i));
      if (bean != null) result.add(bean);
    }
    return result;
  }

  private List<DBObject> getNodesByTags1(List<String> tags) {
    BasicDBObject query = new BasicDBObject("tags", new BasicDBObject("$in", tags));
//    DBCursor cursor = coll.find(query).limit(1000);
    DBCursor cursor = coll.find(query);
    List<DBObject> results = new ArrayList<DBObject>();
    int count = 0;
//    while (cursor.hasNext() && count < 1000) {
    while(cursor.hasNext() ){
      count++;
      results.add(cursor.next());
    }
    return results;
  }

  public List<AdaEventBean> getNodesByTags(final List<String> tags, final int ch, final int method,
      int start, int len, ArrayList<Integer> count) throws Exception {
    StringBuilder key = new StringBuilder("getNodesByTags");
    for (String tag : tags)
      key.append(tag);
    key.append("_").append(ch).append("_").append(method);
    List<DBObject> results = resultCache.get(key.toString(), new Callable<List<DBObject>>() {
      @Override
      public List<DBObject> call() {
        return getNodesByTags(tags, ch, method);
      }
    });
    count.add(results.size());
    List<AdaEventBean> result = new ArrayList<AdaEventBean>();
    for (int i = start; i < start + len && i < results.size(); i++) {
      AdaEventBean bean = getAdaEventBean(results.get(i));
      if (bean != null) result.add(bean);
    }
    return result;
  }

  private List<DBObject> getNodesByTags(List<String> tags, final int ch, final int method) {
    DBCollection collection = db.getCollection("ada_event");// 由于client是个pool，我想collection
                                                            // 放在这里效率会高点，由于collection是线程安全的，若作为全局变量可能会阻塞。
    BasicDBObject query = new BasicDBObject("tags", new BasicDBObject("$in", tags));
    query.put("pt", new BasicDBObject("$lt", new Date()));
    if (ch > 0) query.put("ch", ch);
    if (method > 0) query.put("s", method);
//    DBCursor cursor = collection.find(query).limit(1000).sort(new BasicDBObject("pt", -1));
    DBCursor cursor = collection.find(query).sort(new BasicDBObject("pt", -1));
    List<DBObject> result = new ArrayList<DBObject>();
    while (cursor.hasNext()) {
      result.add(cursor.next());
    }
    return result;
  }

  /**
   * 保证了顺序一致性!
   * 
   * @param ids
   * @return
   * @throws Exception
   */
  public List<AdaEventBean> getNodesByIds(List<Integer> ids) throws Exception {
    BasicDBObject query = new BasicDBObject("_id", new BasicDBObject("$in", ids));
    DBCursor cursor = coll.find(query);
    Map<Integer, AdaEventBean> resultMap = new HashMap<Integer, AdaEventBean>();
    List<AdaEventBean> result = new ArrayList<AdaEventBean>();
    try {
      while (cursor.hasNext()) {
        DBObject eventDoc = cursor.next();
        AdaEventBean bean = getAdaEventBean(eventDoc);
        if (bean != null) resultMap.put(bean.get_id(), bean);
      }
      for (int id : ids) { // return result must has the same order with ids!
        result.add(resultMap.get(id));
      }
    } catch (Exception e) {
      throw new Exception(e);
    } finally {
      cursor.close();
    }
    return result;
  }

  public List<AdaEventBean> getNodesByInsertTime(int start, int len, long st, long et)
      throws Exception {
    Date startDate = new Date(st);
    Date endDate = new Date(et);
    DBCollection collection = db.getCollection("ada_event");// 由于client是个pool，我想collection
                                                            // 放在这里效率会高点，由于collection是线程安全的，若作为全局变量可能会阻塞。
    BasicDBObject query = new BasicDBObject();
    query.put("it", new BasicDBObject("$gt", startDate).append("$lt", endDate));
    DBCursor cursor = collection.find(query);
    int count = 0;
    List<AdaEventBean> result = new ArrayList<AdaEventBean>();
    while (cursor.hasNext() && count < start + len) {
      if (count < start) cursor.next();
      else {
        DBObject eventDoc = cursor.next();
        AdaEventBean bean = getAdaEventBean(eventDoc);
        if (bean != null) result.add(bean);
      }
      count++;
    }
    return result;
  }

  private List<DBObject> getNodesByTimeInOneClass(long st, long et, int ch, int method,
      String timeType) {
    Date startDate = new Date(st);
    Date endDate = new Date(et);
    DBCollection collection = db.getCollection("ada_event");// 由于client是个pool，我想collection
                                                            // 放在这里效率会高点，由于collection是线程安全的，若作为全局变量可能会阻塞。
    BasicDBObject query = new BasicDBObject();
    query.put(timeType, new BasicDBObject("$gt", startDate).append("$lt", endDate));
    if (ch > 0) query.put("ch", ch);
    if (method > 0) query.put("s", method);
    if (ch == 3 && timeType.equals("it")) // 此时不展示子事件
    query.put("cty", 0);

//    DBCursor cursor = collection.find(query).limit(1000).sort(new BasicDBObject(timeType, -1));
    DBCursor cursor = collection.find(query).sort(new BasicDBObject(timeType, -1));
    List<DBObject> result = new ArrayList<DBObject>();
    int count = 0;
    while (cursor.hasNext()) {
      result.add(cursor.next());
      count++;
    }
    return result;
  }

  public List<AdaEventBean> getNodesByInsertTimeInOneClass(int start, int len, final long st,
      final long et, final int ch, final int method, final String timeType, ArrayList<Integer> count)
      throws Exception {
    String key = "getNodesByInsertTimeInOneClass" + st / 3600000 + et / 3600000 + ch + method
        + timeType;
    List<DBObject> results = resultCache.get(key.toString(), new Callable<List<DBObject>>() {
      @Override
      public List<DBObject> call() {
        return getNodesByTimeInOneClass(st, et, ch, method, timeType);
      }
    });
    count.add(results.size());
    List<AdaEventBean> result = new ArrayList<AdaEventBean>();
    for (int i = start; i < start + len && i < results.size(); i++) {
      AdaEventBean bean = getAdaEventBean(results.get(i));
      if (bean != null) result.add(bean);

    }
    return result;
  }

  private AdaEventBean getAdaEventBean(DBObject json) {
    AdaEventBean bean = null;
    try {
      bean = (AdaEventBean) PojoMapper.fromJson(json, AdaEventBean.class);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return bean;
  }

  private List<DBObject> getNodesByTitle(String q) {
    BasicDBObject query = new BasicDBObject();
    query.put("t", q);
    DBCollection collection = db.getCollection("ada_event");
    DBCursor cursor = collection.find(query);
    List<DBObject> result = new ArrayList<DBObject>();
    int count = 0;
//    while (cursor.hasNext() && count < 1000) {
    while (cursor.hasNext()){
      result.add(cursor.next());
      count++;
    }
    return result;

  }

  public List<AdaEventBean> getNodesByTitle(final String q, int start, int len,
      ArrayList<Integer> count) throws Exception {
    if (q == null) return new ArrayList<AdaEventBean>();
    String key = "getNodesByTitle" + q.trim();
    List<DBObject> results = resultCache.get(key.toString(), new Callable<List<DBObject>>() {
      @Override
      public List<DBObject> call() {
        return getNodesByTitle(q.trim());
      }
    });
    count.add(results.size());
    List<AdaEventBean> result = new ArrayList<AdaEventBean>();
    for (int i = start; i < start + len && i < results.size(); i++) {
      AdaEventBean bean = getAdaEventBean(results.get(i));
      if (bean != null) result.add(bean);
    }
    return result;
  }

  private static BasicDBObject getLikeStr(String findStr) {
    Pattern pattern = Pattern.compile("^.*" + findStr + ".*$", Pattern.MULTILINE);
    return new BasicDBObject("$regex", pattern);
  }

  // endWith文件扩展名
  private static BasicDBObject endWithStr(String findStr) {
    Pattern pattern = Pattern.compile(findStr + "$", Pattern.MULTILINE);
    return new BasicDBObject("$regex", pattern);
  }

  // startWith文件扩展名
  protected static BasicDBObject startWithStr(String findStr) {
    Pattern pattern = Pattern.compile("^" + findStr, Pattern.MULTILINE);
    return new BasicDBObject("$regex", pattern);
  }

  public static void main(String[] args) throws Exception {

    AdaEventDao dao = new AdaEventDao();
    long now = System.currentTimeMillis();
    ArrayList<Integer> a = new ArrayList<Integer>(1);
    List<AdaEventBean> result = dao.getNodesByTitle("t", 0, 10, a);
    // List<AdaEventBean> result = dao.getNodesByInsertTime(0, 1000, now - 24 * 3600 * 1000, now);
    System.out.println(a.get(0).intValue());

    // List<String> tags1 = new ArrayList<String>();
    // tags1.add("中国");
    // List<AdaEventBean> aa = dao.getNodesByTags(tags1);
    // System.out.println(aa.get(0).get_id());
  }
}
