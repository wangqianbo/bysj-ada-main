package ict.ada.gdb.rest.services;

import ict.ada.gdb.rest.dao.OpenioDao;
import ict.ada.gdb.rest.util.PojoMapper;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

public class InternalOpenioService {

  public static OpenioDao openioDao = new OpenioDao();

  public static String getOpenioInference(String target) {
    // long start = System.currentTimeMillis();
    // long end = System.currentTimeMillis();
    String ret = null;
    try {
      // long start1 = System.currentTimeMillis();
      ret = PojoMapper.toJson(openioDao.getRelationInfer(target), true);
      // long end1 = System.currentTimeMillis();
      // System.out.println(end-start);
      // System.out.println(end1-start1);
    } catch (JsonMappingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (JsonGenerationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (JsonParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return ret;
  }

  public static String scanOpenioInference(String target, int uid, int start, int len) {
    // long start = System.currentTimeMillis();
    // long end = System.currentTimeMillis();
    String ret = null;
    try {
      // long start1 = System.currentTimeMillis();
      ret = PojoMapper.toJson(openioDao.scanRelationInfer(target, uid, start, len), true);
      // long end1 = System.currentTimeMillis();
      // System.out.println(end-start);
      // System.out.println(end1-start1);
    } catch (JsonMappingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (JsonGenerationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (JsonParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return ret;
  }

  public static void main(String[] args) {
    long start = System.currentTimeMillis();
    System.out.println(InternalOpenioService.getOpenioInference("person"));
    long end = System.currentTimeMillis();
    System.out.println(end - start);
  }
}
