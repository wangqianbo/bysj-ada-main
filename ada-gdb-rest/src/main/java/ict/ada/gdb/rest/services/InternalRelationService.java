package ict.ada.gdb.rest.services;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import ict.ada.common.model.Relation;
import ict.ada.common.model.WdeRef;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.rest.beans.GetRelationInferBean;
import ict.ada.gdb.rest.beans.GetRelationSourceByIdBean;
import ict.ada.gdb.rest.util.EdgeIdConverter;
import ict.ada.gdb.rest.util.NodeIdConveter;
import ict.ada.gdb.rest.util.PojoMapper;
import ict.ada.gdb.rest.util.RelationIdConverter;
import ict.ada.gdb.rest.util.WDEIdConverter;
import ict.ada.gdb.service.AdaGdbService;

public class InternalRelationService {
  static AdaGdbService adaGdbService = InternalServiceResources.getAdaGdbService();
  private static Properties adaGdbRestConf = InternalServiceResources.getAdaGdbRestConf();

  public static String getRelationSourceById(String relationId) {
    String ret = null;
    byte[] id = null;
    Relation rel = null;
    try {
      id = RelationIdConverter.checkAndtoBytes(relationId);
    } catch (Exception e) {
      return EdgeIdConverter.generateErrorCodeJson(e.getMessage());
    }
    try {
      rel = new Relation(id);
      rel = adaGdbService.getRelationDetail(rel);
    } catch (GdbException e) {
      return EdgeIdConverter.generateErrorCodeJson("GdbException happens in query: "
          + e.getMessage());
    }
    GetRelationSourceByIdBean bean = new GetRelationSourceByIdBean();
    List<WdeRef> wderefList = rel.getWdeRefs();
    HashSet<ict.ada.gdb.rest.beans.model.WdeRef> wdeRefbeanSet = new HashSet<ict.ada.gdb.rest.beans.model.WdeRef>(); // 去重，id
                                                                                                                     // length
                                                                                                                     // offset一样的。
    for (WdeRef wderef : wderefList) {
      ict.ada.gdb.rest.beans.model.WdeRef wderefBean = new ict.ada.gdb.rest.beans.model.WdeRef();
      wderefBean.setWdeid(NodeIdConveter.toString(wderef.getWdeId()));
      wderefBean.setOffset(wderef.getOffset());
      wderefBean.setLength(wderef.getLength());
      wderefBean.setType(WDEIdConverter.getChannelIgnoreException(wderef.getWdeId()));
      wdeRefbeanSet.add(wderefBean);
    }
    for (ict.ada.gdb.rest.beans.model.WdeRef wderefBean : wdeRefbeanSet)
      bean.addWderef(wderefBean);

    ret = PojoMapper.toJson(bean, true);

    return ret;
  }

  public static String getRelationInferRulePath(String target) {
    // 在这个地方转义 target
    // TODO 通过client 获取 数据
    // long start = System.currentTimeMillis();
    String json = InternalServiceResources.downloadHtml(adaGdbRestConf
        .getProperty("openio_rest_url") + GetRelationInferBean.GDB2openio.get(target));
    // long end = System.currentTimeMillis();
    // System.out.println(end-start);
    // String json=InternalOpenioService.getOpenioInference(target);
    GetRelationInferBean bean = null;
    String ret = null;
    try {
      bean = new GetRelationInferBean((GetRelationInferBean) PojoMapper.fromJson(json,
          GetRelationInferBean.class), target);
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
    ret = PojoMapper.toJson(bean, true);

    return ret;
  }

  public static void main(String[] args) throws HttpException, IOException {
    System.out
        .println(InternalRelationService
            .getRelationSourceById("153507c801286e1317e0dd1a70cbb48c763901358950a2469278bacc690fa71f82446c4ee5b7a5e4bd9c"));
    Scanner cin = new Scanner(new File("/home/wangqianbo/aaa"), "UTF-8");
    String json = cin.nextLine();
    long start = System.currentTimeMillis();
    // for(int i=0;i<=1000;i++)
    // InternalRelationService.getRelationInferRulePath("web");
    long end = System.currentTimeMillis();
    System.out.println(end - start);
    // System.out.println(args[0]);
    PrintStream out = new PrintStream(new File("/home/wangqianbo/out"), "UTF-8");
    System.setOut(out);
    // System.out.println(InternalRelationService.getRelationInferData(
    // "c801c42a30607f3dc25f03af7d27f178b0b1", json));
    start = System.currentTimeMillis();
    // json="000851f758d05ebe";
    for (int i = 0; i < 1; i++) {
      HttpClient client = new HttpClient();
      GetMethod method = new GetMethod(
          "http://221.0.111.135:24343/ada/node/c80c5b6b5ad30d7563951a916e17b61554e9/inference");
      // method.addRequestHeader("RULE", json);
      // client.executeMethod(method);
      // System.out.println(new String(method.getResponseBody(), "UTF-8"));
      // method.getResponseBody();
    }
    end = System.currentTimeMillis();
    System.out.println(end - start);
  }
}
