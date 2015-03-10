package ict.ada.gdb.rest.dao;

import ict.ada.gdb.rest.dao.bean.AdaEventBean;
import ict.ada.gdb.rest.dao.bean.AdaIdentityBean;
import ict.ada.gdb.rest.util.PojoMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class AdaIdentityDao {
  private final static String ADAIDENTITYNAME = "ada_identity";
  private static Cache<String, List<DBObject>> resultCache = CacheBuilder.newBuilder()
      .concurrencyLevel(4).maximumSize(100).expireAfterWrite(10, TimeUnit.MINUTES).build();

  // 这个缓存系统是不是应该统一，太分散不好管理。
  private List<DBObject> getIdentityByTags(List<String> tags) throws Exception {
    BasicDBObject query = new BasicDBObject("tags", new BasicDBObject("$in", tags));
    DBCursor cursor = MongoDBManager.getDbCollection(ADAIDENTITYNAME).find(query);
    List<DBObject> result = new ArrayList<DBObject>();
    try {
      while (cursor.hasNext()) {
        result.add(cursor.next());
      }
    } catch (Exception e) {
      throw new Exception(e);
    } finally {
      cursor.close();
    }
    return result;

  }

  /**
   * @param tags
   *          用于查询的tags
   * @param start
   *          用于分页
   * @param len
   *          　　
   * @param count
   *          　results总的数目，用于分页
   * @return　　　　　分页后结果。
   * @throws ExecutionException
   */
  public List<AdaIdentityBean> getIdentityByTags(final List<String> tags, int start, int len,
      ArrayList<Integer> count) throws ExecutionException {
    if (tags == null || tags.size() == 0) return Collections.emptyList();
    String key = "getIdentityByTags" + tags.toString();
    List<DBObject> results = resultCache.get(key, new Callable<List<DBObject>>() {
      @Override
      public List<DBObject> call() throws Exception {
        return getIdentityByTags(tags);
      }
    });
    count.add(results.size());
    if (start >= results.size()) return Collections.emptyList();
    List<AdaIdentityBean> result = new ArrayList<AdaIdentityBean>(len);
    for (int i = start; i < results.size() && (i - start) < len; i++)
      try {
        result.add((AdaIdentityBean) PojoMapper.fromJson(results.get(i), AdaIdentityBean.class));
      } catch (JsonMappingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (JsonParseException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    return result;
  }

  public AdaIdentityBean getIdentityById(String id) {
    if (id == null) return AdaIdentityBean.NULLBEAN;
    BasicDBObject query = new BasicDBObject("_id", id);
    DBCursor cursor = MongoDBManager.getDbCollection(ADAIDENTITYNAME).find(query);
    AdaIdentityBean result = new AdaIdentityBean();
    if (cursor.hasNext()) try {
      result = (AdaIdentityBean) PojoMapper.fromJson(cursor.next(), AdaIdentityBean.class);
    } catch (Exception e) { // 像这种 Exception 应如何处理？
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return result;
  }

  public static void main(String[] args) {
    /*
     * DBObject object=new BasicDBObject("_id","e6033976b8d29378ef79fb8a1f0b2ed33c98");
     * object.put("n", "e603李开复"); object.put("sn", "李开复"); object.put("addl", "addl"); List<String>
     * tags=new ArrayList<String>(); tags.add("李开复"); object.put("tags", tags); List<String>
     * ents=new ArrayList<String>(); ents.add("c8014377bbdb5ff333304ebfb17ab365d26b");
     * ents.add("c80658edf2621e70af433ce391f37c882a0c");
     * ents.add("c80c4377bbdb5ff333304ebfb17ab365d26b");
     * ents.add("c8049b2f885df58498d6fa93603f84c932d3"); object.put("ents", ents); String[]
     * doc={"070851ff5bfd9b0a","060851ff57a42f19"}; List<String>
     * docs=Lists.asList("070851ff4254b38d",doc); object.put("docs", docs); DBObject query=new
     * BasicDBObject("_id","e6033976b8d29378ef79fb8a1f0b2ed33c98");
     * MongoDBManager.getDbCollection(ADAIDENTITYNAME).update(query,object, true,false);
     */
    AdaIdentityDao dao = new AdaIdentityDao();
    List<String> tags = new ArrayList<String>();
    tags.add("李开复");
    try {
      List<DBObject> re = dao.getIdentityByTags(tags);
      System.out.println(re.size());
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
}
