import java.io.IOException;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

public class Test {

  /**
   * @param args
   */
  public static void main(String[] args) {
MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
    connectionManager.setMaxConnectionsPerHost(10);
HttpClient client = new HttpClient(connectionManager);
    StringBuffer body = new StringBuffer();

    GetMethod method = new GetMethod("http://221.0.111.140:5802/ada/wderefs/details"); // Create a method instance.
    String json="{\"details\" : [ { \"id\" : \"0301531087f00000\", \"len\" : 27,\"off\" : 12 }, { \"id\" : \"06015310400e0000\", \"len\" : 26,\"off\" : 206 }, { \"id\" : \"0801531122790000\", \"len\" : 26,\"off\" : 204 }]}";
   
      method.addRequestHeader("DETAILS", json);
   
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
      body.append(new String(responseBody, "UTF-8"));
      if (statusCode == HttpStatus.SC_NOT_FOUND) {
      System.out.println("the service is not available now!");
      } else if (statusCode != HttpStatus.SC_OK) { System.out.println("the service encounter a problem:"
          + new String(responseBody, "UTF-8")); }

    } catch (HttpException e) {
      System.err.println("Fatal protocol violation: " + e.getMessage());
      e.printStackTrace();
      System.out.println("Fatal protocol violation: " + e.getMessage());
    } catch (IOException e) {
      System.err.println("Fatal transport error: " + e.getMessage());
      e.printStackTrace();
      System.out.println("Fatal transport error: " + e.getMessage());
    } finally {
      // Release the connection.
      method.releaseConnection();
    }
    System.out.println( body.toString());
  

  }

}
