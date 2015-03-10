package ict.ada.common.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.HttpURLConnection;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class GeoAddress {

  static private int timeout = 20000;

  static private String userAgent = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.4 (KHTML, like Gecko) "
      + "Chrome/22.0.1229.95 Safari/537.4";

  /**
   * Get the web page content based on the url.
   * 
   * @param url
   *          the URL, must not be null.
   * @return Web page contents or null if error occurs.
   */
  static private String getJsonGeoAddr(String url) {
    HttpURLConnection httpConnection;
    URL URL;
    int code;

    StringBuffer stringBuffer = new StringBuffer();

    if (url.length() == 0) {
      return null;
    } else if (!url.startsWith("http://")) {
      url = "http://" + url;
    } else if (url.startsWith("http://")) {
      // XXX we leave here nothing on purpose.
    } else
      return null;

    try {
      URL = new URL(url);
      httpConnection = (HttpURLConnection) URL.openConnection();
      httpConnection.setRequestMethod("GET");
      // httpConnection.setReadTimeout(timeout);
      httpConnection.setConnectTimeout(timeout);
      httpConnection.setRequestProperty("User-Agent", userAgent);
      httpConnection.setDoInput(true);
      code = httpConnection.getResponseCode();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    if (code == HttpURLConnection.HTTP_OK) {
      try {
        String strCurrentLine;
        BufferedReader reader = new BufferedReader(new InputStreamReader(
            httpConnection.getInputStream()));
        while ((strCurrentLine = reader.readLine()) != null) {
          stringBuffer.append(strCurrentLine).append("\n");
        }
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }

    return stringBuffer.toString();
  }

  static private String getJsonGeoAddr(String url, String agent) {
    HttpURLConnection httpConnection;
    URL URL;
    int code;

    StringBuffer stringBuffer = new StringBuffer();

    if (url.length() == 0) {
      return null;
    } else if (!url.startsWith("http://")) {
      url = "http://" + url;
    } else if (url.startsWith("http://")) {
      // XXX we leave here nothing on purpose.
    } else
      return null;

    try {
      URL = new URL(url);
      httpConnection = (HttpURLConnection) URL.openConnection();
      httpConnection.setRequestMethod("GET");
      // httpConnection.setReadTimeout(timeout);
      httpConnection.setConnectTimeout(timeout);
      httpConnection.setRequestProperty("User-Agent", agent);
      httpConnection.setDoInput(true);
      code = httpConnection.getResponseCode();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    if (code == HttpURLConnection.HTTP_OK) {
      try {
        String strCurrentLine;
        BufferedReader reader = new BufferedReader(new InputStreamReader(
            httpConnection.getInputStream()));
        while ((strCurrentLine = reader.readLine()) != null) {
          stringBuffer.append(strCurrentLine).append("\n");
        }
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }

    return stringBuffer.toString();
  }

  private static String[] parseLatLong(String json) {
    String[] latLng = new String[2];
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode rootNode = mapper.readValue(json, JsonNode.class);
      JsonNode nodeStatus = rootNode.get("status");
      if (nodeStatus.getValueAsText().equals("OK")) {
        JsonNode resultsNode = rootNode.get("results").get(0);
        latLng[0] = String.valueOf(resultsNode.path("geometry").path("location").path("lat").getDoubleValue());
        latLng[1] = String.valueOf(resultsNode.path("geometry").path("location").path("lng").getDoubleValue());
      }
    } catch (JsonParseException e) {
      e.printStackTrace();
    } catch (JsonMappingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return latLng;
  }
  
  /**
   * return the latitude and longitude of a address.
   * 
   * @param addr
   *          address.
   * @return the latitude and longitude of a address., latLng[0]:
   *         latitude,latLng[1]: longitude.
   */
  public static String[] getCoordinate(String addr) {
    String[] latLng = null;
    String address = null;
    try {
      address = java.net.URLEncoder.encode(addr, "UTF-8");
    } catch (UnsupportedEncodingException e1) {
      e1.printStackTrace();
    }
    String url = "http://maps.google.com/maps/api/geocode/json?address=" + address + "&sensor=true";
    String addrJson = getJsonGeoAddr(url);
    latLng = parseLatLong(addrJson);
    return latLng;
  }
}
