package ict.ada.gdb.rest.services;

import java.util.Arrays;
import java.util.List;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.Relation;
import ict.ada.common.util.Pair;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.TimeRange;
import ict.ada.gdb.rest.beans.GetEdgeInfomationByIdBean;
import ict.ada.gdb.rest.beans.GetEdgeTimeLineBean;
import ict.ada.gdb.rest.beans.model.EdgeInfo;
import ict.ada.gdb.rest.util.EdgeIdConverter;
import ict.ada.gdb.rest.util.NodeIdConveter;
import ict.ada.gdb.rest.util.PojoMapper;
import ict.ada.gdb.service.AdaGdbService;

public class InternalEdgeService {
  static AdaGdbService adaGdbService = InternalServiceResources.getAdaGdbService();

  static String getEdgeInfomationById(String edgeId) {
    String ret = null;
    byte[] id = null;
    Edge edge = null;
    try {
      id = EdgeIdConverter.checkAndtoBytes(edgeId);
    } catch (Exception e) {
      return EdgeIdConverter.generateErrorCodeJson(e.getMessage());
    }
    List<Pair<Integer, List<Relation>>> result = null;
    try {
      edge = new Edge(new Node(Arrays.copyOfRange(id, 0, Node.NODEID_SIZE)), new Node(
          Arrays.copyOfRange(id, Node.NODEID_SIZE, 2 * Node.NODEID_SIZE)));
      result = adaGdbService.getEdgeRelations(edge, null, null, true);
    } catch (GdbException e) {
      return EdgeIdConverter.generateErrorCodeJson("GdbException happens in query: "
          + e.getMessage());
    }
    GetEdgeInfomationByIdBean bean = new GetEdgeInfomationByIdBean();
    for (Relation relation : result.get(0).getSecond()) {
      EdgeInfo edgeinfo = new EdgeInfo();
      edgeinfo.setRelation_id(NodeIdConveter.toString(relation.getId()));
      edgeinfo.setType(relation.getType().toString()); // TODO integer Type to String Type
      edgeinfo.setWeight(relation.getWeight());
      bean.addInfos(edgeinfo);
    }
    ret = PojoMapper.toJson(bean, true);

    return ret;
  }

  public static String getEdgeTimeLine(String edgeId, int st, int et) {
    String ret = null;
    byte[] id = null;
    Edge edge = null;
    st = st == -1 ? 0 : st;
    et = et == -1 ? Integer.MAX_VALUE : et;
    System.out.println("st= " + st + " et= " + et);
    try {
      id = EdgeIdConverter.checkAndtoBytes(edgeId);
    } catch (Exception e) {
      return EdgeIdConverter.generateErrorCodeJson(e.getMessage());
    }
    List<Pair<Integer, List<Relation>>> result = null;
    try {
      edge = new Edge(new Node(Arrays.copyOfRange(id, 0, Node.NODEID_SIZE)), new Node(
          Arrays.copyOfRange(id, Node.NODEID_SIZE, 2 * Node.NODEID_SIZE)));
      result = adaGdbService.getEdgeRelations(edge, null, new TimeRange(st, et), false);
      System.out.println("result:::" + result.size());
    } catch (GdbException e) {
      return EdgeIdConverter.generateErrorCodeJson("GdbException happens in query: "
          + e.getMessage());
    }
    GetEdgeTimeLineBean bean = new GetEdgeTimeLineBean(result);
    ret = PojoMapper.toJson(bean, true);
    return ret;

  }
}
