package ict.ada.gdb.rest.services;

import ict.ada.common.util.Pair;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.rest.dao.AdaEventDao;
import ict.ada.gdb.rest.dao.GdbRestHTablePool;
import ict.ada.gdb.rest.dao.HBaseEventDAO;
import ict.ada.gdb.rest.util.PojoMapper;
import ict.ada.gdb.service.AdaGdbService;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.digester.plugins.strategies.LoaderSetProperties;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import cn.golaxy.yqpt2.dtsearch2.client.DTSearchClient;

/**
 * because InternalService have many static method so have many share methods and resources, put
 * them here.所有的share资源应该放在这里。可以尽量避免冲突。
 * */

@SuppressWarnings("deprecation")
public class InternalServiceResources {

  private static final String UTF_8 = "UTF-8";
  private static AdaGdbService adaGdbService = new AdaGdbService(AdaModeConfig.GDBMode.QUERY);
  private static MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
  private static Properties channelService = new Properties();
  private static Properties adaGdbRestConf = new Properties();
  private static DTSearchClient SearchClient = new DTSearchClient();
  private static HashSet<String> subordinateType = new HashSet<String>();
  private static HashSet<String> superiorType = new HashSet<String>();
  private static HashSet<String> ancestryType = new HashSet<String>();
  private static List<Integer> methodIntType = new ArrayList<Integer>();
  private static List<String> methodStringType = new ArrayList<String>();
  private static List<String> methodName = new ArrayList<String>();
  private static List<Integer> channelIntType = new ArrayList<Integer>();
  private static List<String> channelStringType = new ArrayList<String>();
  private static List<String> channelName = new ArrayList<String>();
  private static ExecutorService exec = new ThreadPoolExecutor(40, Integer.MAX_VALUE, 60L,
      TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
  static {
    connectionManager.setMaxConnectionsPerHost(10);
    try {
      InputStream in = LoaderSetProperties.class.getClassLoader().getResourceAsStream(
          "channel_service.properties");
      channelService.load(in);
      in = LoaderSetProperties.class.getClassLoader()
          .getResourceAsStream("ada_gdb_rest.properties");
      adaGdbRestConf.load(in);
      // Event properties
      for(String item : adaGdbRestConf.getProperty("method_int_type").split(",")){
        methodIntType.add(Integer.parseInt(item));
      }
      Collections.addAll(methodStringType, adaGdbRestConf.getProperty("method_string_type").split(","));
      Collections.addAll(methodName, adaGdbRestConf.getProperty("method_name").split(","));
      
      for(String item : adaGdbRestConf.getProperty("channel_int_type").split(",")){
        channelIntType.add(Integer.parseInt(item));
      }
      Collections.addAll(channelStringType, adaGdbRestConf.getProperty("channel_string_type").split(","));
      Collections.addAll(channelName, adaGdbRestConf.getProperty("channel_name").split(","));
      
      SearchClient.SetDTService(adaGdbRestConf.getProperty("master_ip_port"));
      Scanner cin = new Scanner(new File(adaGdbRestConf.getProperty("subordinate_file")), "UTF-8");
      while (cin.hasNext()) {
        String line = cin.nextLine().trim();
        subordinateType.add(line);
        ancestryType.add(line);
	System.out.println(line);
      }
      cin.close();
      cin = new Scanner(new File(adaGdbRestConf.getProperty("supeior_file")), "UTF-8");
      while (cin.hasNext()) {
        String line = cin.nextLine().trim();
        superiorType.add(line);
        ancestryType.add(line);
      }
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  private static HttpClient client = new HttpClient(connectionManager);
  private static AdaEventDao adaEventDao = new AdaEventDao();
  private static HBaseEventDAO eventRelDocDao;
  static {
    GdbRestHTablePool pool = null;
    try {
      pool = new GdbRestHTablePool(adaGdbRestConf.getProperty("ada_event_doc_rel_htable"));
      // TODO when exception happens
    } catch (ZooKeeperConnectionException e) {
      e.printStackTrace();
    }
    eventRelDocDao = new HBaseEventDAO(pool);
  }

  public static HttpClient getHttpClient() {
    return client;
  }
  public static ExecutorService getExecutorService(){
    return  exec;
  }
  public static Properties getChannelService() {
    return channelService;
  }

  public static DTSearchClient getDTSearchClient() {
    return SearchClient;
  }

  public static AdaGdbService getAdaGdbService() {
    return adaGdbService;
  }

  public static Properties getAdaGdbRestConf() {
    return adaGdbRestConf;
  }

  public static AdaEventDao getAdaEventDao() {
    return adaEventDao;
  }

  public static HBaseEventDAO getEventRelDocDao() {
    return eventRelDocDao;
  }

  public static Collection<String> getSubordinateType() {
    return subordinateType;
  }

  public static Collection<String> getSuperiorType() {
    return superiorType;
  }

  public static Collection<String> getAncestryType() {
    return ancestryType;
  }
  
  public static List<Integer> getMethodIntType(){
    return methodIntType;
  }

  public static List<String> getMethodStringType(){
    return methodStringType;
  }
  
  public static List<String> getMethodName(){
    return methodName;
  }
  
  public static List<Integer> getChannelIntType(){
    return channelIntType;
  }

  public static List<String> getChannelStringType(){
    return channelStringType;
  }
  
  public static List<String> getChannelName(){
    return channelName;
  }
  /**
   * static method of HttpClient Get method from a url
   * 
   * @param url
   * <br>
   *          the url to get
   * @return the content get from the url.
   */
  public static String downloadHtml(String url) {
    StringBuffer body = new StringBuffer();
    GetMethod method = new GetMethod(url); // Create a method instance.
    method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
        new DefaultHttpMethodRetryHandler(3, false));// Provide custom
    // retry handler
    // is necessary
    try {
      int statusCode = client.executeMethod(method);// Execute the method.
      byte[] responseBody = method.getResponseBody();// Read the response
      // body.
      // Deal with the response.
      // Use caution: ensure correct character encoding and is not binary
      // data
      body.append(new String(responseBody, UTF_8));
      if (statusCode == HttpStatus.SC_NOT_FOUND) {
        return generateErrorCodeJson("the service is not available now!");
      } else if (statusCode != HttpStatus.SC_OK) { return generateErrorCodeJson("the service encounter a problem:"
          + new String(responseBody, UTF_8)); }

    } catch (HttpException e) {
      System.err.println("Fatal protocol violation: " + e.getMessage());
      e.printStackTrace();
      return generateErrorCodeJson("Fatal protocol violation: " + e.getMessage());
    } catch (IOException e) {
      System.err.println("Fatal transport error: " + e.getMessage());
      e.printStackTrace();
      return generateErrorCodeJson("Fatal transport error: " + e.getMessage());
    } finally {
      // Release the connection.
      method.releaseConnection();
    }
    return body.toString();
  }

  public static String downloadHtml(String url, List<Pair<String, String>> headers) {
    StringBuffer body = new StringBuffer();

    GetMethod method = new GetMethod(url); // Create a method instance.
    for (Pair<String, String> header : headers) {
      method.addRequestHeader(header.getFirst(), header.getSecond());
    }
    method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
        new DefaultHttpMethodRetryHandler(3, false));// Provide custom
    // retry handler
    // is necessary
    try {
      int statusCode = client.executeMethod(method);// Execute the method.
      byte[] responseBody = method.getResponseBody();// Read the response
      // body.
      // Deal with the response.
      // Use caution: ensure correct character encoding and is not binary
      // data
      body.append(new String(responseBody, UTF_8));
      if (statusCode == HttpStatus.SC_NOT_FOUND) {
        return generateErrorCodeJson("the service is not available now!");
      } else if (statusCode != HttpStatus.SC_OK) { return generateErrorCodeJson("the service encounter a problem:"
          + new String(responseBody, UTF_8)); }

    } catch (HttpException e) {
      System.err.println("Fatal protocol violation: " + e.getMessage());
      e.printStackTrace();
      return generateErrorCodeJson("Fatal protocol violation: " + e.getMessage());
    } catch (IOException e) {
      System.err.println("Fatal transport error: " + e.getMessage());
      e.printStackTrace();
      return generateErrorCodeJson("Fatal transport error: " + e.getMessage());
    } finally {
      // Release the connection.
      method.releaseConnection();
    }
    return body.toString();
  }

  public static String postToWeb(String queryString, String url) {
    StringRequestEntity requestEntity = null;
    try {
      requestEntity = new StringRequestEntity(queryString, "application/json", "UTF-8");
    } catch (UnsupportedEncodingException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    StringBuffer body = new StringBuffer();
    PostMethod post = new PostMethod(url);
    post.setRequestEntity(requestEntity);
    try {
      int statusCode = client.executeMethod(post);// Execute the method.
      byte[] responseBody = post.getResponseBody();// Read the response
      // body.
      // Deal with the response.
      // Use caution: ensure correct character encoding and is not binary
      // data
      body.append(new String(responseBody, UTF_8));
      if (statusCode == HttpStatus.SC_NOT_FOUND) {
        return generateErrorCodeJson("the service is not available now!");
      } else if (statusCode != HttpStatus.SC_OK) { return generateErrorCodeJson("the service encounter a problem:"
          + new String(responseBody, UTF_8)); }

    } catch (HttpException e) {
      System.err.println("Fatal protocol violation: " + e.getMessage());
      e.printStackTrace();
      return generateErrorCodeJson("Fatal protocol violation: " + e.getMessage());
    } catch (IOException e) {
      System.err.println("Fatal transport error: " + e.getMessage());
      e.printStackTrace();
      return generateErrorCodeJson("Fatal transport error: " + e.getMessage());
    } finally {
      // Release the connection.
      post.releaseConnection();
    }
    return body.toString();

  }

  /**
   * when a exception happens in a rest api,use this method to generate a json style error message
   * to return
   * 
   * @param errorCode
   * <br>
   *          what you want put in the errorCode of message
   * @return a json style error message
   */
  public static String generateErrorCodeJson(String errorCode) {
    Map<String, String> errorCodeMap = new HashMap<String, String>();
    if (errorCode.isEmpty()) errorCode = "errorCode";
    errorCodeMap.put("errorCode", errorCode);
    String errorCodeJson = PojoMapper.toJson(errorCodeMap, true); // assert
    // no
    // exception
    // happens
    return errorCodeJson;
  }

  public static void main(String[] args) {
    "".trim();
    // System.out.println(getEventNodeName("test",0,100));
    System.out
        .println(downloadHtml("http://172.22.0.28:9494/?cmd=findallPaths&start=c802bfdfee233a336cc87f685898367b93ce&end=c80202fad5d86ae23f505111e5ef4ded33bc&depth=6&limits=64"));
  }

}
