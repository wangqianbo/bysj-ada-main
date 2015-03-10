package ict.ada.gdb.rest.services;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.rest.beans.GetEventListBean;
import ict.ada.gdb.rest.beans.GetEventNodeNameBean;
import ict.ada.gdb.rest.beans.GetReleventEventsBean;
import ict.ada.gdb.rest.beans.GetReleventEventsMBean;
import ict.ada.gdb.rest.common.MinHeap;
import ict.ada.gdb.rest.common.ReleventEvent;
import ict.ada.gdb.rest.common.ReleventEventBean;
import ict.ada.gdb.rest.dao.AdaEventDao;
import ict.ada.gdb.rest.dao.bean.AdaEventBean;
import ict.ada.gdb.rest.util.EventUtil;
import ict.ada.gdb.rest.util.PojoMapper;
import ict.ada.gdb.service.AdaGdbService;
import ict.ada.gdb.util.NodeIdConveter;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.javatuples.Pair;
import org.javatuples.Triplet;

public class InternalEventService {
  private static AdaEventDao adaEventDao = InternalServiceResources.getAdaEventDao();
  private static AdaGdbService adaGdbService = InternalServiceResources.getAdaGdbService();

  public static String getEventNodeName(String tag, int start, int len)
      throws JsonMappingException, JsonGenerationException, IOException {
    List<String> tags = new ArrayList<String>(); // TODO tag可以指定多个，以一定的格式隔开
    tags.add(tag);
    String ret = null;
    GetEventNodeNameBean bean = null;
    ArrayList<Integer> count = new ArrayList<Integer>(1);
    try {
      List<AdaEventBean> adaEvents = adaEventDao.getNodesByTags(tags, start, len, count);
      bean = new GetEventNodeNameBean(adaEvents);
      bean.setCount(count.get(0));
    } catch (Exception e) {
      return InternalServiceResources.generateErrorCodeJson("GdbException happens in query: "
          + e.getMessage());
    }
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getEventNodeName(String tag, String channels, String methods, int start,
      int len) {
    String res = null;
    List<String> tags = new ArrayList<String>(); // TODO tag可以指定多个，以一定的格式隔开
    tags.add(tag);
    GetEventListBean bean = new GetEventListBean();
    if (channels.equals("NOEXISTS") && methods.equals("NOEXISTS"))
      for (int channelIntType : EventUtil.channelIntType) {
      List<AdaEventBean> result = null;
      // int methodIntType=EventUtil.getMethodIntType(method);
      ArrayList<Integer> count = new ArrayList<Integer>(1);
      try {
        result = adaEventDao.getNodesByTags(tags, channelIntType, -1, start, len, count);
        if (result.size() > 0) bean.addResult(result, channelIntType, -1, count.get(0));
      } catch (Exception e) {
        return InternalServiceResources.generateErrorCodeJson("wrong query : " + e.getMessage());
      }

    }

    else if (!channels.equals("NOEXISTS")) {
      for (String channel : channels.split(",")) {
        List<AdaEventBean> result = null;
        int channelIntType = EventUtil.getChannelIntType(channel);
        // int methodIntType=EventUtil.getMethodIntType(method);
        ArrayList<Integer> count = new ArrayList<Integer>(1);
        try {
          result = adaEventDao.getNodesByTags(tags, channelIntType, -1, start, len, count);
          if (result.size() > 0) bean.addResult(result, channelIntType, -1, count.get(0));
        } catch (Exception e) {
          return InternalServiceResources.generateErrorCodeJson("wrong query : " + e.getMessage());
        }
      }
    } else {
      for (String method : methods.split(",")) {
        List<AdaEventBean> result = null;
        int methodIntType = EventUtil.getMethodIntType(method);
        // int methodIntType=EventUtil.getMethodIntType(method);
        ArrayList<Integer> count = new ArrayList<Integer>(1);
        try {
          result = adaEventDao.getNodesByTags(tags, -1, methodIntType, start, len, count);
          if (result.size() > 0) bean.addResult(result, -1, methodIntType, count.get(0));
        } catch (Exception e) {
          return InternalServiceResources.generateErrorCodeJson("wrong query : " + e.getMessage());
        }
      }
    }
    res = PojoMapper.toJson(bean, true);
    return res;
  }

  public static String getEventListInMultipleClasses(int start, int len, long st, long et,
      String channels, String methods, String timeType) {
    long now = System.currentTimeMillis();
    String res = null;
    if (st == -1) st = now - 24 * 3600 * 1000;
    if (et == -1) et = now;
    if (et <= st) return InternalServiceResources
        .generateErrorCodeJson("illegal arguments : endtime < starttime");
    if (channels.equals("NOEXISTS") && methods.equals("NOEXISTS")) return InternalServiceResources
        .generateErrorCodeJson("illegal arguments : Both channels and  methods  are null ");
    GetEventListBean bean = new GetEventListBean();
    if (!channels.equals("NOEXISTS")) {
      for (String channel : channels.split(",")) {
        List<AdaEventBean> result = null;
        int channelIntType = EventUtil.getChannelIntType(channel);
        // int methodIntType=EventUtil.getMethodIntType(method);
        ArrayList<Integer> count = new ArrayList<Integer>(1);
        try {
          result = adaEventDao.getNodesByInsertTimeInOneClass(start, len, st, et, channelIntType,
              -1, timeType, count);
          if (result.size() > 0) bean.addResult(result, channelIntType, -1, count.get(0));
        } catch (Exception e) {
          return InternalServiceResources.generateErrorCodeJson("wrong query : " + e.getMessage());
        }
      }
    } else {
      for (String method : methods.split(",")) {
        List<AdaEventBean> result = null;
        int methodIntType = EventUtil.getMethodIntType(method);
        // int methodIntType=EventUtil.getMethodIntType(method);
        ArrayList<Integer> count = new ArrayList<Integer>(1);
        try {
          result = adaEventDao.getNodesByInsertTimeInOneClass(start, len, st, et, -1,
              methodIntType, timeType, count);
          if (result.size() > 0) bean.addResult(result, -1, methodIntType, count.get(0));
        } catch (Exception e) {
          return InternalServiceResources.generateErrorCodeJson("wrong query : " + e.getMessage());
        }
      }
    }
    res = PojoMapper.toJson(bean, true);
    return res;
  }

  public static String getEventList(int start, int len, long st, long et, String channel,
      String method, String timeType) {
    long now = System.currentTimeMillis();
    int channelIntType = EventUtil.getChannelIntType(channel);
    int methodIntType = EventUtil.getMethodIntType(method);
    if (channelIntType == -1 && methodIntType == -1) return InternalServiceResources
        .generateErrorCodeJson("illegal arguments : Both channel and  method  are illegal ");
    String res = null;
    if (st == -1) st = now - 24 * 3600 * 1000;
    if (et == -1) et = now;
    if (et <= st) return InternalServiceResources
        .generateErrorCodeJson("illegal arguments : endtime < starttime");
    List<AdaEventBean> result = null;
    ArrayList<Integer> count = new ArrayList<Integer>(1);
    try {
      result = adaEventDao.getNodesByInsertTimeInOneClass(start, len, st, et, channelIntType,
          methodIntType, timeType, count);
    } catch (Exception e) {
      return InternalServiceResources.generateErrorCodeJson("wrong query : " + e.getMessage());
    }
    GetEventListBean bean = new GetEventListBean(result, channelIntType, methodIntType,
        count.get(0));

    res = PojoMapper.toJson(bean, true);
    return res;
  }

  public static String getEventByTitle(String q, int start, int len) {
    String ret = null;
    List<AdaEventBean> result = null;
    ArrayList<Integer> count = new ArrayList<Integer>(1);
    try {
      result = adaEventDao.getNodesByTitle(q, start, len, count);
    } catch (Exception e) {
      return InternalServiceResources.generateErrorCodeJson("wrong query : " + e.getMessage());
    }
    GetEventNodeNameBean bean = new GetEventNodeNameBean(result);
    bean.setCount(count.get(0));
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  /**
   * @param eventId
   * @param types
   * @param len
   * @return
   */
  public static String getReleventEvents(String eventId, List<String> types, int len) {
    String json = null;
    byte[] nodeId = null;
    HashSet<Attribute> nodeTypeSet = new HashSet<Attribute>(types.size());
    try {
      if (eventId.length() != Node.NODEID_SIZE * 2) {
        byte[] type = StringUtils.hexStringToByte(eventId.substring(0, 4));
        try {
          NodeType nodeType = NodeType.getType(type[0], type[1]);
          Channel channel = nodeType.getChannel();
          Attribute attribute = nodeType.getAttribute();
          // System.out.println(channel);
          nodeId = adaGdbService.getNodeIdByName(eventId, NodeType.getType(channel, attribute));
          if(nodeId == null){
            GetReleventEventsBean bean = new GetReleventEventsBean(len);
            json = PojoMapper.toJson(bean, true);
            return json;
          }
        } catch (Exception e) {
          return InternalServiceResources.generateErrorCodeJson(e.getMessage());
        }
      }

      else nodeId = NodeIdConveter.checkAndtoBytes(eventId);
      for (String type : types) {
        // System.out.println(type);
        if (type.equals("all")) for (Attribute nodeType : Attribute.values()) {
          if (nodeType != Attribute.EVENT && nodeType != Attribute.ANY) nodeTypeSet.add(nodeType);
        }
        else nodeTypeSet.add(NodeTypeMapper.getAttribute(type));
      }
    } catch (Exception e) {
      return InternalServiceResources.generateErrorCodeJson(e.getMessage());
    }
    Node startEvent = new Node(nodeId);
    ReleventEvent releventEvent = new ReleventEvent(startEvent, nodeTypeSet, adaGdbService, len);
    try {
      releventEvent.genSimilarEvents();
      InternalNodeService.addNodeNameForNodes(releventEvent.getElements());
    } catch (GdbException e) {
      return InternalServiceResources.generateErrorCodeJson("wrong query " + e.getMessage());
    }
    List<Triplet<Node, List<Node>, Integer>> result = releventEvent.getSimilarEvents();
    HashMap<Node, AdaEventBean> nodeToEventMap = new HashMap<Node, AdaEventBean>(len);
    List<Node> nodes = new ArrayList<Node>(len);
    for (Triplet<Node, List<Node>, Integer> triplet : result)
      nodes.add(triplet.getValue0());
    try {
      List<AdaEventBean> eventBeans = getEventByNodeId(nodes);
      Iterator<AdaEventBean> eventIter = eventBeans.iterator();
      for (Node node : nodes) {
        nodeToEventMap.put(node, eventIter.next());
      }
      GetReleventEventsBean bean = new GetReleventEventsBean(len);
      for (Triplet<Node, List<Node>, Integer> similarEvent : result) {
        AdaEventBean adaEventbean = nodeToEventMap.get(similarEvent.getValue0());
        if (adaEventbean != null) bean.addEvent(adaEventbean, similarEvent.getValue1(),
            similarEvent.getValue2());
      }
      json = PojoMapper.toJson(bean, true);
      return json;
    } catch (GdbException e) {
      return InternalServiceResources.generateErrorCodeJson("wrong query " + e.getMessage());
    }

  }

  /**
   * @fun 去除无用的 tags
   * @param tags
   * @return
   */
  private static Set<String> removeTags(Collection<String> tags) {
    Set<String> ret = new HashSet<String>();
    for (Object tag : tags) {
      String categoryName = tag.toString();
      if (!(categoryName.endsWith("语句") || categoryName.endsWith("分类")
          || categoryName.endsWith("条目") || categoryName.endsWith("页面")
          || categoryName.endsWith("作品") || categoryName.endsWith("事件")
          || categoryName.endsWith("消岐页") || categoryName.endsWith("新闻动态") || categoryName
            .endsWith("动态列表"))) ret.add(categoryName);
    }
    return ret;
  }

  /**
   * @fun 给定 tagA,tagB,计算 sim=交集/全集
   * @param tagA
   * @param tagB
   * @return
   */
  private static Pair<Set<String>, Double> getSimilarity(Set<String> tagA, Set<String> tagB) {
    double ret = 0;
    Set<String> tagSub = new HashSet<String>(tagA);
    Set<String> tagUnion = new HashSet<String>(tagA);
    tagSub.retainAll(tagB);
    tagUnion.addAll(tagB);
    ret = (1.0 * tagSub.size()) / (tagUnion.size() * 1.0);
    // return (double)tagAtmp.size();
    return new Pair<Set<String>, Double>(tagSub, ret);
  }

  public static String getReleventEvents(int eventId, int len) {
    List<Integer> ids = new ArrayList<Integer>(1);
    ids.add(eventId);

    try {
      List<AdaEventBean> iniEventR = adaEventDao.getNodesByIds(ids);
      if (iniEventR == null || iniEventR.size() == 0) return null; // TODO
      AdaEventBean iniEvent = iniEventR.get(0);
      Set<String> iniTags = removeTags(iniEvent.getTags());
      iniTags.addAll(iniEvent.getTags());
      List<AdaEventBean> simiEvents = adaEventDao.getNodesByTags(iniTags);
      ReleventEventBean[] heap = new ReleventEventBean[len + 1];

      MinHeap<ReleventEventBean> resultHeap = new MinHeap<ReleventEventBean>(heap, len,
          new Comparator<ReleventEventBean>() {
            public int compare(ReleventEventBean s1, ReleventEventBean s2) {
              double flag = s1.getScore() - s2.getScore();
              if (flag > 0) return 1;
              else if (flag == 0) return 0;
              else return -1;
            }

          });

      for (AdaEventBean event : simiEvents) {
        if (event.get_id() != iniEvent.get_id()) {
          Pair<Set<String>, Double> similarity = getSimilarity(iniTags, removeTags(event.getTags()));
          resultHeap.addElement(new ReleventEventBean(event, similarity.getValue0(), similarity
              .getValue1()));
        }
      }
      GetReleventEventsMBean bean = new GetReleventEventsMBean(resultHeap.getSortedHeap());
      return PojoMapper.toJson(bean, true);
    } catch (Exception e) {
      e.printStackTrace();
      return InternalServiceResources.generateErrorCodeJson(e.getMessage());
    }

  }

  /**
   * 
   * 保证顺序的一致性!
   * 
   * @param nodes
   * @return
   * @throws GdbException
   */
  private static List<AdaEventBean> getEventByNodeId(List<Node> nodes) throws GdbException {
    List<AdaEventBean> result = new ArrayList<AdaEventBean>(nodes.size());
    List<byte[]> nodeIds = new ArrayList<byte[]>(nodes.size());
    for (Node node : nodes)
      nodeIds.add(node.getId());
    List<Integer> eventIds = new ArrayList<Integer>();
    List<ict.ada.common.util.Pair<String, List<String>>> eventNames = adaGdbService
        .getNodeNameAndSnameByIdBatched(nodeIds, nodeIds.size() / 16 + 1);
    boolean[] flag = new boolean[eventNames.size()];
    int index = 0;
    for (ict.ada.common.util.Pair<String, List<String>> eventName : eventNames) {
      try {
        String eventId = eventName.getFirst().substring(4);
        if (eventId.matches("^[0-9]+$")) {
          eventIds.add(Integer.parseInt(eventId));
          flag[index] = true;
        } else flag[index] = false;
        index++;
      } catch (Exception e) {
        flag[index++] = false;
      }
    }
    List<AdaEventBean> events = null;
    try {
      events = adaEventDao.getNodesByIds(eventIds);// 能够保证顺序的一致.!
    } catch (Exception e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    index = 0;
    for (AdaEventBean bean : events) {
      if (flag[index++]) result.add(bean);
      else result.add(null);
    }
    return result;
  }

  public static void main(String[] args) {
    long now = System.currentTimeMillis();
    // System.out.println(InternalEventService.getEventList(0, 20, now-10*24*3600*1000,
    // now,"baike","sss"));
  }
}
