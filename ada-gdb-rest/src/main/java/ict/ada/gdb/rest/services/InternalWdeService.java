package ict.ada.gdb.rest.services;

import ict.ada.common.model.Event;
import ict.ada.common.model.Node;
import ict.ada.common.model.WdeRef;
import ict.ada.common.util.Pair;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.TimeRange;
import ict.ada.gdb.rest.beans.GetHtml_urlBean;
import ict.ada.gdb.rest.beans.GetWdeRefDetailBean;
import ict.ada.gdb.rest.beans.GetWdeRefsByIdBean;
import ict.ada.gdb.rest.beans.QueryWdeRefDetailsBean;
import ict.ada.gdb.rest.beans.ScholarWDEDetailBean;
import ict.ada.gdb.rest.beans.WDEDetailBean;
import ict.ada.gdb.rest.beans.WDEDetailBeanError;
import ict.ada.gdb.rest.dao.HBaseEventDAO;
import ict.ada.gdb.rest.util.NodeIdConveter;
import ict.ada.gdb.rest.util.PojoMapper;
import ict.ada.gdb.service.AdaGdbService;
import ict.ada.gdb.util.ParallelTask;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.util.HttpURLConnection;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

//import org.apache.tools.ant.taskdefs.LoadProperties;

@SuppressWarnings("deprecation")
public class InternalWdeService {
  private static HBaseEventDAO eventRelDocDao = InternalServiceResources.getEventRelDocDao();
  private static AdaGdbService adaGdbService = InternalServiceResources.getAdaGdbService();
  private static ExecutorService exec = InternalServiceResources.getExecutorService();
  public static final String UTF_8 = "UTF-8";
  public static Properties channelService = InternalServiceResources.getChannelService();
  // private static String host = channelService.getProperty("host");
  private static HttpClient client = InternalServiceResources.getHttpClient();

  public static String getDetail(String id, String offset, String len) {
    // TODO 保证 id start len 格式正确！
    StringBuilder queryString = new StringBuilder();
    String ret = null;
    String channel = String.valueOf(Integer.parseInt(id.substring(2, 4), 16));
    // channelService.getProperty(channel);
    queryString.append(channelService.getProperty(channel)).append("get_detail?");
    queryString.append("id=").append(id).append("&offset=").append(offset).append("&len=")
        .append(len);
    System.out.println(queryString.toString());
    String orjson = downloadHtml(queryString.toString());
    WDEDetailBean bean = null;
    if (channel.equals("100")) {

      try {
        bean = new WDEDetailBean((ScholarWDEDetailBean) PojoMapper.fromJson(orjson,
            ScholarWDEDetailBean.class));
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
    } else try {
      bean = (WDEDetailBean) PojoMapper.fromJson(orjson, WDEDetailBean.class);
    } catch (Exception e) {
      WDEDetailBeanError errorBean = new WDEDetailBeanError();
      try {
        errorBean = (WDEDetailBeanError) PojoMapper.fromJson(orjson, WDEDetailBeanError.class);
      } catch (Exception e1) {
        return orjson;
      }
      bean = new WDEDetailBean(errorBean);
    }

    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static WDEDetailBean getDetail1(String id, int offset, int len) {
    // TODO 保证 id start len 格式正确！
    StringBuilder queryString = new StringBuilder();
    String ret = null;
    String channel = String.valueOf(Integer.parseInt(id.substring(2, 4), 16));
    // channelService.getProperty(channel);
    queryString.append(channelService.getProperty(channel)).append("get_detail?");
    queryString.append("id=").append(id).append("&offset=").append(offset).append("&len=")
        .append(len);
    System.out.println(queryString.toString());
    String orjson = downloadHtml(queryString.toString());
    WDEDetailBean bean = null;
    if (channel.equals("100")) {

      try {
        bean = new WDEDetailBean((ScholarWDEDetailBean) PojoMapper.fromJson(orjson,
            ScholarWDEDetailBean.class));
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
    } else try {
      bean = (WDEDetailBean) PojoMapper.fromJson(orjson, WDEDetailBean.class);
    } catch (Exception e) {
      WDEDetailBeanError errorBean = new WDEDetailBeanError();
      try {
        errorBean = (WDEDetailBeanError) PojoMapper.fromJson(orjson, WDEDetailBeanError.class);
      } catch (Exception e1) {
        return null;
      }
      bean = new WDEDetailBean(errorBean);
    }

    return bean;
  }
  
  public static List<WDEDetailBean> getDetails1( List<WdeRef> wderefs) throws Exception{
    final List<WDEDetailBean> wdeBeans = new ArrayList<WDEDetailBean>();
    ParallelTask<WDEDetailBean> pTask = new ParallelTask<WDEDetailBean>(
        exec) {
      @Override
      public void processResult(WDEDetailBean result) {
        if(result != null){
          wdeBeans.add(result);
        }
      }
    };
    for(final WdeRef wderef : wderefs){
      pTask.submitTasks(new Callable<WDEDetailBean>(){
        public WDEDetailBean call(){
          return getDetail1(NodeIdConveter.toString(wderef.getWdeId()),wderef.getOffset(),wderef.getLength());
        }
      });
    }
    pTask.gatherResults();
    return wdeBeans;
  }
  static String getDetails(String wdeRefs) {
    try {
      Pair<String, String> header = new Pair<String, String>("DETAILS", wdeRefs);
      List<Pair<String, String>> headers = new ArrayList<Pair<String, String>>(1);
      headers.add(header);
      QueryWdeRefDetailsBean query = (QueryWdeRefDetailsBean) PojoMapper.fromJson(wdeRefs,
          QueryWdeRefDetailsBean.class);
      StringBuilder queryString = new StringBuilder();
      String channel = "1";
      if (query.getDetails() == null || query.getDetails().size() == 0) {
      } else {
        channel = String.valueOf(Integer.parseInt(
            query.getDetails().get(0).getId().substring(2, 4), 16));
      }
      queryString.append(channelService.getProperty(channel)).append("get_details");
      return InternalServiceResources.downloadHtml(queryString.toString(), headers);
    } catch (Exception e) {
      e.printStackTrace();
      return InternalServiceResources.generateErrorCodeJson(e.getMessage());
    }
  }

  public static String getContent(String id) {
    StringBuilder queryString = new StringBuilder();
    String channel = String.valueOf(Integer.parseInt(id.substring(2, 4), 16));
    channelService.getProperty(channel);
    queryString.append(channelService.getProperty(channel)).append("get_content?");
    queryString.append("id=").append(id);
    return downloadHtml(queryString.toString());
  }

  public static String getHtml(String id) {
    StringBuilder queryString = new StringBuilder();
    String channel = String.valueOf(Integer.parseInt(id.substring(2, 4), 16));
    channelService.getProperty(channel);
    queryString.append(channelService.getProperty(channel).trim()).append("get_html?");
    queryString.append("id=").append(id);
    return downloadHtml(queryString.toString());
  }

  public static String getHtml_url(String id) {
    StringBuilder queryString = new StringBuilder();
    String channel = String.valueOf(Integer.parseInt(id.substring(2, 4), 16));
    channelService.getProperty(channel);
    queryString.append(channelService.getProperty(channel).trim()).append("get_html?");
    queryString.append("id=").append(id);
    String ret = null;
    GetHtml_urlBean bean = new GetHtml_urlBean(queryString.toString());
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  private static String downloadHtml(String url) {
    StringBuffer body = new StringBuffer();
    GetMethod method = new GetMethod(url); // Create a method instance.
    method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
        new DefaultHttpMethodRetryHandler(3, false));// Provide custom retry handler is necessary
    try {
      int statusCode = client.executeMethod(method);// Execute the method.
      if (statusCode != HttpStatus.SC_OK) { return body.append("{").append("\n")
          .append("  \"errorCode\" : \"the service is not available now!\"").append("\n")
          .append("}").toString(); }
      byte[] responseBody = method.getResponseBody();// Read the response body.
      // Deal with the response.
      // Use caution: ensure correct character encoding and is not binary data
      body.append(new String(responseBody, UTF_8));
    } catch (HttpException e) {
      System.err.println("Fatal protocol violation: " + e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      System.err.println("Fatal transport error: " + e.getMessage());
      e.printStackTrace();
    } finally {
      // Release the connection.
      method.releaseConnection();
    }
    return body.toString();
  }

  public static String getWdeRefsById(String id, int start, int len) {

    Node node = null;
    Event event = null;
    String ret = null;
    start = start < 0 ? 0 : start;
    GetWdeRefsByIdBean bean = new GetWdeRefsByIdBean();
    if (id.length() == Node.NODEID_SIZE * 2)// the length of id
    {
      byte[] nodeId = null;

      try {
        nodeId = NodeIdConveter.checkAndtoBytes(id);
      } catch (Exception e) {
        return InternalServiceResources.generateErrorCodeJson(e.getMessage());
      }
      try {
        node = new Node(nodeId);
        adaGdbService.getNodeWdeRefs(node, TimeRange.ANY_TIME);
      } catch (GdbException e) {
        return InternalServiceResources.generateErrorCodeJson("GdbException happens in query: "
            + e.getMessage());
      }
      int length = node.getWdeRefs().size();
      while (length - start > 0 && len > 0) {
        bean.addWdeRef(node.getWdeRefs().get(length - start - 1));// 取倒序。时间从大到小
        start++;
        len--;
      }
      /*
       * for (WdeRef wderef : node.getWdeRefs()) { if (start > 0) start--; else if (len > 0) {
       * bean.addWdeRef(wderef); len--; } else break; }
       */
      bean.setTotal(node.getWdeRefs().size());
    } else {
      int eventId = -1;
      try {
        eventId = Integer.parseInt(id);
      } catch (Exception e) {
        return InternalServiceResources.generateErrorCodeJson("Wrong id :" + id);

      }
      try {
        event = new Event(eventId);
        eventRelDocDao.getEventWdeRefs(event, TimeRange.ANY_TIME);
      } catch (GdbException e) {
        return InternalServiceResources.generateErrorCodeJson("GdbException happens in query: "
            + e.getMessage());
      }
      /*
       * for (WdeRef wderef : event.getWdeRefs()) { if (start > 0) start--; else if (len > 0) {
       * bean.addWdeRef(wderef); len--; } else break; }
       */
      int length = event.getWdeRefs().size();
      while (length - start > 0 && len > 0) {
        bean.addWdeRef(event.getWdeRefs().get(length - start - 1));// 取倒序。时间从大到小
        start++;
        len--;
      }
      bean.setTotal(event.getWdeRefs().size());
    }
    ret = PojoMapper.toJson(bean, true);
    return ret;

  }

  public static String getEventWdeRefsById(String id, int start, int len) {
    Event event = null;
    String ret = null;
    start = start < 0 ? 0 : start;
    GetWdeRefsByIdBean bean = new GetWdeRefsByIdBean();
    int eventId = -1;
    try {
      eventId = Integer.parseInt(id);
    } catch (Exception e) {
      return InternalServiceResources.generateErrorCodeJson("Wrong id :" + id);

    }
    try {
      event = new Event(eventId);
      eventRelDocDao.getEventWdeRefs(event, TimeRange.ANY_TIME);
    } catch (GdbException e) {
      return InternalServiceResources.generateErrorCodeJson("GdbException happens in query: "
          + e.getMessage());
    }
    int length = event.getWdeRefs().size();
    while (length - start > 0 && len > 0) {
      bean.addWdeRef(event.getWdeRefs().get(length - start - 1));// 取倒序。时间从大到小
      start++;
      len--;
    }
    bean.setTotal(event.getWdeRefs().size());
    ret = PojoMapper.toJson(bean, true);
    return ret;

  }

  public static String getEventWdeRefsById1(String id, int start, int len) {
    Event event = null;
    String ret = null;
    start = start < 0 ? 0 : start;
    GetWdeRefDetailBean bean = new GetWdeRefDetailBean();
    int eventId = -1;
    try {
      eventId = Integer.parseInt(id);
    } catch (Exception e) {
      return InternalServiceResources.generateErrorCodeJson("Wrong id :" + id);

    }
    try {
      event = new Event(eventId);
      eventRelDocDao.getEventWdeRefs(event, TimeRange.ANY_TIME);
    } catch (GdbException e) {
      return InternalServiceResources.generateErrorCodeJson("GdbException happens in query: "
          + e.getMessage());
    }
    int length = event.getWdeRefs().size();
    List<WdeRef> wderefs = new ArrayList<WdeRef>(len);
    while (length - start > 0 && len > 0) {
      wderefs.add(event.getWdeRefs().get(length - start - 1));// 取倒序。时间从大到小
      start++;
      len--;
    }
    try {
      bean.setWderefs(InternalWdeService.getDetails1(wderefs));
      bean.setTotal(event.getWdeRefs().size());
    } catch (Exception e) {
      return InternalServiceResources.generateErrorCodeJson("Get Wde Detail fail: " + e.getMessage());
    }
    ret = PojoMapper.toJson(bean, true);
    return ret;

  }
  
  @Deprecated
  private static String downloadHtml1(String url) {
    URL u;
    InputStream is = null;
    DataInputStream dis;
    String s;
    StringBuffer sb = new StringBuffer();

    try {
      u = new URL(url);
      HttpURLConnection uc = (HttpURLConnection) u.openConnection();
      // uc.connect();
      // System.out.println(uc.getContentType());
      // System.out.println((String)uc.getContent());
      is = u.openStream();
      dis = new DataInputStream(new BufferedInputStream(is));
      while ((s = dis.readLine()) != null) {
        sb.append(s + "\n");
      }
    } catch (MalformedURLException mue) {
      System.out.println("Ouch - a MalformedURLException happened.");
      mue.printStackTrace();
      return sb.append("{").append("\n")
          .append("  \"errorCode\" : \"the service is not available now!\"").append("\n")
          .append("}").toString();
    } catch (IOException ioe) {
      return sb.append("{").append("\n")
          .append("  \"errorCode\" : \"the service is not available now!\"").append("\n")
          .append("}").toString();
    } finally {
      try {
        if (is != null) is.close();
      } catch (IOException ioe) {
      }
    }
    return sb.toString();
  }

  public static void main(String[] args) {
    // System.out.println(InternalWdeService.getDetail("050851eb242106b6", "952", "37"));
    // System.out.println(InternalWdeService.getContent("00c9521ea0608a47"));
    // System.out.println(InternalWdeService.getContent("000851e5aa9f530d"));
    // System.out.println(InternalWdeService.downloadHtml("http://10.61.1.11:8080/gdbservice/wde/dd/get_detail?id=0064000000000003&start=0&len=0"));
    System.out.println(InternalWdeService.getDetail("0064000000000003", "0", "0"));
  }
}
