package ict.ada.gdb.rest.util;

import java.io.IOException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

public class InternalHttpClient {
  private static MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();

  public static final String UTF_8 = "UTF-8";
  static {
    connectionManager.setMaxConnectionsPerHost(10);
  }
  private static HttpClient client = new HttpClient(connectionManager);

}
