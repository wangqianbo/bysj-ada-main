package ict.ada.gdb.rest.dao;

import ict.ada.gdb.rest.beans.GetRelationInferBean;
import ict.ada.gdb.rest.beans.OpenioRuleBean;
import ict.ada.gdb.rest.beans.GetRelationInferBean.Rule;
import ict.ada.gdb.rest.beans.OpenioRuleBean1;
import ict.ada.gdb.rest.beans.ScanRelationInferBean;
import ict.ada.gdb.rest.services.InternalNodeService;
import ict.ada.gdb.rest.services.InternalServiceResources;
import ict.ada.gdb.rest.util.PojoMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

public class OpenioDao {

  private String driver = "com.mysql.jdbc.Driver";
  private static Properties adaGdbRestConf = InternalServiceResources.getAdaGdbRestConf();
  private static String url = adaGdbRestConf.getProperty("openio_db_url");
  private static String user = adaGdbRestConf.getProperty("openio_db_user");
  private static String password = adaGdbRestConf.getProperty("openio_db_password");
  public ResultSet rs = null;
  static {
    MysqlConnetionPool.setUrl(url);
    MysqlConnetionPool.setUser(user);
    MysqlConnetionPool.setPassword(password);
  }

  public GetRelationInferBean getRelationInfer(String target) throws JsonMappingException,
      JsonParseException, IOException {
    Statement statement = null;
    ResultSet resultset = null;
    Connection conn = null;
    GetRelationInferBean bean = new GetRelationInferBean();
    String sql = "select inference_rel,description,score,rule from rules where target = \'"
        + target + "\' and rule is not NULL";
    try {
      conn = MysqlConnetionPool.getConnection();
      statement = conn.createStatement();
      resultset = statement.executeQuery(sql);
      while (resultset.next()) {
        Rule rule = new Rule();
        rule.setDescription(resultset.getString(2));
        rule.setInference_name(resultset.getString(1));
        rule.setScore(String.valueOf(resultset.getInt(3)));
        String json = resultset.getString(4);
        rule.setRule(((OpenioRuleBean) PojoMapper.fromJson(json, OpenioRuleBean.class)).getRule());
        bean.addRule(rule);
      }
      bean.setErrorCode("success");
      resultset.close();
      conn.close();
    } catch (SQLException e) {
      // TODO 处理Exception
      e.printStackTrace();
      bean.setErrorCode("SQLException:" + e.getMessage());
    }
    return bean;
  }

  public ScanRelationInferBean scanRelationInfer(String target, int uid, int start, int len)
      throws JsonMappingException, JsonParseException, IOException {
    Statement statement = null;
    ResultSet resultset = null;
    Connection conn = null;
    ScanRelationInferBean bean = new ScanRelationInferBean();
    String sql = null;

    if (uid != -1) sql = "select inference_rel,description,score,rule,user_id from rules_new  where target = \'"
        + target + "\'  and user_id =  " + uid + " and rule is not NULL";
    else sql = "select inference_rel,description,score,rule,user_id from rules_new  where target = \'"
        + target + "\'   and rule is not NULL";
    start = start < 0 ? 0 : start;
    len = len < 0 ? 0 : len;
    int count1 = 0;
    try {
      conn = MysqlConnetionPool.getConnection();
      statement = conn.createStatement();
      resultset = statement.executeQuery(sql);
      while (resultset.next()) {
        count1++;
        if (count1 > start && count1 <= (start + len)) {
          ScanRelationInferBean.Rule rule = new ScanRelationInferBean.Rule();
          rule.setDescription(resultset.getString(2));
          rule.setInference_name(resultset.getString(1));
          rule.setScore(String.valueOf(resultset.getInt(3)));
          String json = resultset.getString(4);
          rule.setUid(resultset.getInt(5));
          rule.setRule(((OpenioRuleBean1) PojoMapper.fromJson(json, OpenioRuleBean1.class))
              .getRule());
          bean.addRule(rule);
        }

      }
      bean.setCount(count1);
      bean.setErrorCode("success");
      resultset.close();
      conn.close();
    } catch (SQLException e) {
      // TODO 处理Exception
      e.printStackTrace();
      bean.setErrorCode("SQLException:" + e.getMessage());
    }
    return bean;
  }
}
