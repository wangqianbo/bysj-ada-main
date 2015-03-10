package ict.ada.gdb.rest.util;

import ict.ada.gdb.rest.services.InternalServiceResources;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

// import com.google.code.geocoder.model.*;

public class GeoCodeBean {

  public String address;
  public String source;
  public JsonNode result;

  public GeoCodeBean() {

  }

  public GeoCodeBean(String address, String source) {
    this.address = address;
    this.source = source;
    this.result = null;
  }

  public GeoCodeBean(String address, String source, JsonNode result) {
    this.address = address;
    this.source = source;
    this.result = result;
  }

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public JsonNode getResult() {
    return result;
  }

  public void setResult(JsonNode result) {
    this.result = result;
  }

  public static List<GeoCodeBean> query(QueryBean queryBean) {
    String queryJson = PojoMapper.toJson(queryBean, false);
    String url = InternalServiceResources.getAdaGdbRestConf().getProperty("ada_geocode_service");
    String result = InternalServiceResources.postToWeb(queryJson, url);
    try {
      GeoCodeBean[] resultBeans = (GeoCodeBean[]) PojoMapper.fromJson(result, GeoCodeBean[].class);
      return Arrays.asList(resultBeans);
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
    return null;
  }

  public static class QueryBean {

    // {"address": ["北京", "上海"], "source": "google"}

    public List<String> address;
    public String source;

    // need it for Jackson
    public QueryBean() {

    }

    public QueryBean(List<String> address, String source) {
      this.address = address;
      this.source = source;
    }

    public List<String> getAddress() {
      return address;
    }

    public void setAddress(List<String> address) {
      this.address = address;
    }

    public String getSource() {
      return source;
    }

    public void setSource(String source) {
      this.source = source;
    }
  }
}