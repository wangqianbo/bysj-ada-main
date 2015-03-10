package ict.ada.gdb.rest.services;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.NodeAttribute;
import ict.ada.common.model.NodeAttribute.AttrValueInfo;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.common.model.PathGraph;
import ict.ada.common.model.Relation;
import ict.ada.common.model.RelationGraph;
import ict.ada.common.model.RelationType;
import ict.ada.common.model.WdeRef;
import ict.ada.common.util.Pair;
import ict.ada.common.util.Timer;
import ict.ada.dataprocess.community.CommunityDiscovery;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.PathQuerySpec;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.common.TimeRange;
import ict.ada.gdb.rest.beans.GetAttributeSourceByIdBean;
import ict.ada.gdb.rest.beans.GetAttributesByIdBean;
import ict.ada.gdb.rest.beans.GetCliquesOfNodeBean;
import ict.ada.gdb.rest.beans.GetEventTrackInfoBean;
import ict.ada.gdb.rest.beans.GetGcommunityBean;
import ict.ada.gdb.rest.beans.GetMapBean;
import ict.ada.gdb.rest.beans.GetNodeActionInfoBean;
import ict.ada.gdb.rest.beans.GetNodeActionRefInfoBean;
import ict.ada.gdb.rest.beans.GetNodeClusterCBean;
import ict.ada.gdb.rest.beans.GetNodeEntsBean;
import ict.ada.gdb.rest.beans.GetNodeHierarchyByIdBean;
import ict.ada.gdb.rest.beans.GetNodeIdByNameBean;
import ict.ada.gdb.rest.beans.GetNodeNameByIdBean;
import ict.ada.gdb.rest.beans.GetNodeNameFromInternalIndexBean;
import ict.ada.gdb.rest.beans.GetNodeRelNodeClusterByIdBean;
import ict.ada.gdb.rest.beans.GetNodesRelRelativeNodesBean;
import ict.ada.gdb.rest.beans.GetPathBetNodes2Bean;
import ict.ada.gdb.rest.beans.GetPathBetNodesBean;
import ict.ada.gdb.rest.beans.GetRelationByNodeIdBean;
import ict.ada.gdb.rest.beans.GetRelationInferBean.Rule.Regulation;
import ict.ada.gdb.rest.beans.GetRelationTimeLineByNodeIdBean;
import ict.ada.gdb.rest.beans.GetReleventNodesBean;
import ict.ada.gdb.rest.beans.GetTowLevelRelationGraphBean;
import ict.ada.gdb.rest.beans.GetWdeRefDetailBean;
import ict.ada.gdb.rest.beans.NodeStatistic;
import ict.ada.gdb.rest.beans.NodesPathBean;
import ict.ada.gdb.rest.beans.OpenioRuleBean;
import ict.ada.gdb.rest.common.Clique;
import ict.ada.gdb.rest.common.DfsPathGraph;
import ict.ada.gdb.rest.common.MaxClique;
import ict.ada.gdb.rest.common.Path;
import ict.ada.gdb.rest.common.ReleventNodeSearcher;
import ict.ada.gdb.rest.dao.AdaEventDao;
import ict.ada.gdb.rest.dao.bean.AdaEventBean;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.rest.util.EdgeIdConverter;
import ict.ada.gdb.rest.util.GeoCodeBean;
import ict.ada.gdb.rest.util.GeoCodeBean.QueryBean;
import ict.ada.gdb.rest.util.NodeIdConveter;
import ict.ada.gdb.rest.util.PojoMapper;
import ict.ada.gdb.rest.util.WDEIdConverter;
import ict.ada.gdb.rest.util.WDEUtil;
import ict.ada.gdb.service.AdaGdbService;
import ict.software.ada.community.ModularityHierarchCommunitySearch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.javatuples.Triplet;

import cn.golaxy.yqpt2.dtsearch2.client.DTSearchClient;
import cn.golaxy.yqpt2.dtsearch2.client.DTSearchDoc;
import cn.golaxy.yqpt2.dtsearch2.client.DTSearchResult;

public class InternalNodeService {
  static AdaGdbService adaGdbService = InternalServiceResources.getAdaGdbService();
  private static AdaEventDao adaEventDao = InternalServiceResources.getAdaEventDao();
  private static Properties channelService = InternalServiceResources.getChannelService();
  private static ExecutorService exec = InternalServiceResources.getExecutorService();
  static DTSearchClient client = InternalServiceResources.getDTSearchClient();// client每次查询都要新建一个socket,因此不存在线程是否安全的问题.

  // static DTSearchClient client = new DTSearchClient();
  // static {
  // client.SetDTService("10.61.2.161:8050");
  // }

  public static String getNodeNameFromInternalIndex(final String name, String channelTypes,
      String attributeTypes, final int start, final int len ,final boolean opt) {
    GetNodeNameFromInternalIndexBean bean = new GetNodeNameFromInternalIndexBean();
    String ret = null;
    // TODO channelTypes attributeTypes 合法性
    LinkedHashSet<Channel> channels = new LinkedHashSet<Channel>();
    final LinkedHashSet<Attribute> attributes = new LinkedHashSet<Attribute>();
    for (String channelname : channelTypes.split(","))
      if (channelname.equals("all")) channels.addAll(NodeTypeMapper.getMapperChannels());
      else {
        Channel channel = NodeTypeMapper.getChannel(channelname);
        if (channel != null) channels.add(channel);
      }
    for (String attributename : attributeTypes.split(","))
      if (attributename.equals("all")) attributes.addAll(NodeTypeMapper.getMapperAttributes());
      else {
        Attribute attribute = NodeTypeMapper.getAttribute(attributename);
        if (attribute != null) attributes.add(attribute);
      }

    final List<String> fields = new LinkedList<String>();
    fields.add("name");
    fields.add("sname");
    fields.add("addl");
    List<Future<Pair<Channel, List<Triplet<String, Long, List<DTSearchDoc>>>>>> resultList = new ArrayList<Future<Pair<Channel, List<Triplet<String, Long, List<DTSearchDoc>>>>>>();
    System.out.println(channels);
    for (final Channel channel : channels) {
      Future<Pair<Channel, List<Triplet<String, Long, List<DTSearchDoc>>>>> result = exec
          .submit(new Callable<Pair<Channel, List<Triplet<String, Long, List<DTSearchDoc>>>>>() {

            @Override
            public Pair<Channel, List<Triplet<String, Long, List<DTSearchDoc>>>> call()
                throws Exception {
              short indexType = (short) adaGdbService.getIndexNum(channel);
              List<Triplet<String, Long, List<DTSearchDoc>>> result1 = new ArrayList<Triplet<String, Long, List<DTSearchDoc>>>();
              ;
              for (Attribute attribute : attributes) {
                int nodeType = NodeType.getType(channel, attribute).getIntegerForm();
                String query = "[FIELD]( type, [FILTER]( [FIELD]( sname, \"" + name + "\" ), ==, "
                    + nodeType + " ) )";
                // System.out.println(query);
                DTSearchResult result = new DTSearchResult(); // Search Result
                if(opt){
                  client.Search(indexType, query, fields, 0, 1000, false, result);
                }
                else{
                  client.Search(indexType, query, fields, start, len, false, result);
                }
                switch (result.ret_code) {
                case 0x02:
                  // NONE_SUCC
                  break;
                case 0x01:
                  // PART_SUCC
                  break;
                case 0x00:
                  // ALL_SUCC
                  break;
                default:
                  break;
                }
                if (result.ret_code != 0x02) {
                  if (result.docs != null && result.docs.size() != 0) {
                    if(opt){
                      LinkedHashMap<String,DTSearchDoc> uniq = new LinkedHashMap<String,DTSearchDoc>();
                      for(DTSearchDoc doc : result.docs){
                        if(uniq.containsKey(doc.fields.get(0))){
                          if(uniq.get(doc.fields.get(0)).fields.get(1).length() < doc.fields.get(1).length()){
                            uniq.put(doc.fields.get(0), doc);
                          }
                        }
                        else{
                          uniq.put(doc.fields.get(0), doc);
                        }
                      }
                      List<DTSearchDoc> uniqList = new ArrayList<DTSearchDoc>();
                      int count = 0;
                      for(DTSearchDoc doc : uniq.values()){
                        if(count < start){
                          count++;
                          continue;
                        }else if(count - start+1 > len ){
                          break;
                        }else{
                          uniqList.add(doc);
                          count++;
                        }
                      }
                      result1.add(new Triplet<String, Long, List<DTSearchDoc>>(NodeTypeMapper.getAttributeName(attribute), (long) uniq.size(), uniqList));
                    }else{
                      result1.add(new Triplet<String, Long, List<DTSearchDoc>>(NodeTypeMapper.getAttributeName(attribute), result.matchs, result.docs));
                    }
                  }
                }

              }
              return new Pair<Channel, List<Triplet<String, Long, List<DTSearchDoc>>>>(channel,
                  result1);
            }

          });
      resultList.add(result);
    }
    for (Future<Pair<Channel, List<Triplet<String, Long, List<DTSearchDoc>>>>> future : resultList) {
      Pair<Channel, List<Triplet<String, Long, List<DTSearchDoc>>>> futureResult = null;
      try {
        futureResult = future.get();
      } catch (InterruptedException e) {

        e.printStackTrace();
      } catch (ExecutionException e) {

        e.printStackTrace();
      }
      System.out.println(futureResult.getFirst());
      List<Triplet<String, Long, List<DTSearchDoc>>> result1 = futureResult.getSecond();
      if (result1 != null && result1.size() > 0) bean.addChannel(
          NodeTypeMapper.getChannelName(futureResult.getFirst()), result1);
    }

    try {
      ret = PojoMapper.toJson(bean, true);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ret;
  }

  // 传过来的name是否会有前缀呢?
  public static String getNodeIdByName(String name) {
    byte[] id = null;
    String ret = null;
    String stringId = null;
    Channel channel = null;
    Attribute attribute = null;
    if (name == null) { return generateErrorCodeJson("name is null"); }
    byte[] type = StringUtils.hexStringToByte(name.substring(0, 4));
    try {
      NodeType nodeType = NodeType.getType(type[0], type[1]);
      channel = nodeType.getChannel();
      attribute = nodeType.getAttribute();

    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    try {
      NodeType nodeType = NodeType.getType(channel, attribute);
      id = adaGdbService.getNodeIdByName(name, nodeType);
      if (id != null) {
        if (Bytes.equals(id, 0, 2, nodeType.getBytesForm(), 0, 2)) stringId = NodeIdConveter
            .toString(id);
        else return generateErrorCodeJson("name not exists in this type");
      } else return generateErrorCodeJson("name not exists in this type");
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    GetNodeIdByNameBean bean = new GetNodeIdByNameBean();
    ict.ada.gdb.rest.beans.model.Node node = new ict.ada.gdb.rest.beans.model.Node(stringId, name,
        null, NodeTypeMapper.getChannelName(channel), NodeTypeMapper.getAttributeName(attribute),
        null);
    bean.setNode(node);
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getNodeEnts(String nodeId){
    Node node = null;
    String ret = null;
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
      try {
        node = adaGdbService.getNodeEntsById(id);
        GetNodeEntsBean bean = new GetNodeEntsBean(node);
        ret = PojoMapper.toJson(bean, false);
      } catch (GdbException e) {
        return generateErrorCodeJson(e.getMessage());
      }
      return ret;
  }

  static String getAttributesById(String nodeId, boolean disambiguation, String wdeIds,
      boolean source,boolean all) {
    Node nodeAttr = null;
    String ret = null;
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    try {
      Node node = new Node(id);
      if (disambiguation) nodeAttr = adaGdbService.getNodeAttributes(node,
          getWdeIdsFromStringList(wdeIds));
      else nodeAttr = adaGdbService.getNodeAttributes(node, source|(!all));
      if(!all){
        Node nodeAttrFilter = new Node(id);
        for(NodeAttribute attr : nodeAttr.getAttributes()){
          Iterator<AttrValueInfo> iter = attr.getValues().iterator();
          AttrValueInfo valueInfo = iter.next();
          int maxTS = WDEUtil.getLatestTimeStamp(valueInfo.getWdeRefs());
          while(iter.hasNext()){
            AttrValueInfo valueInfo2 = iter.next();
            if(maxTS <  WDEUtil.getLatestTimeStamp(valueInfo2.getWdeRefs())){
              maxTS =  WDEUtil.getLatestTimeStamp(valueInfo2.getWdeRefs());
              valueInfo = valueInfo2;
            }
          }
          if(!source){
            valueInfo = new AttrValueInfo(valueInfo.getValue(),null);
          }
          List<AttrValueInfo> infoList = new ArrayList<AttrValueInfo>(1);
          infoList.add(valueInfo);
          NodeAttribute newAttr = new NodeAttribute(attr.getKey(),infoList);
          nodeAttrFilter.addNodeAttribute(newAttr);
        }
        nodeAttr = nodeAttrFilter;
      }
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    
    GetAttributesByIdBean bean = getAttributesInter(nodeAttr);
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static GetAttributesByIdBean getAttributesInter(Node nodeAttr) {
    GetAttributesByIdBean bean = new GetAttributesByIdBean();
    bean.initAttrsList();
    if (nodeAttr.getAttributes() != null) {
      for (NodeAttribute nodeAttribute : nodeAttr.getAttributes()) {
        String key = nodeAttribute.getKey();
        for (AttrValueInfo valueInfo : nodeAttribute.getValues()) {
          ict.ada.gdb.rest.beans.model.NodeAttribute attr = new ict.ada.gdb.rest.beans.model.NodeAttribute();
          attr.setKey(key);
          attr.setValue(valueInfo.getValue());
          attr.setCount(valueInfo.getWdeRefCount());

          List<ict.ada.gdb.rest.beans.model.WdeRef> wdeRefs = null;
          if (valueInfo.getWdeRefs() != null && valueInfo.getWdeRefs().size() != 0) {
            wdeRefs = new ArrayList<ict.ada.gdb.rest.beans.model.WdeRef>(valueInfo.getWdeRefs()
                .size());
            for (WdeRef wdeRef : valueInfo.getWdeRefs()) {
              wdeRefs.add(new ict.ada.gdb.rest.beans.model.WdeRef(wdeRef));
            }
          }
          attr.setWdeRefs(wdeRefs);
          bean.addAttrsList(attr);
        }
      }
    }
    bean.setErrorCode("success");
    return bean;
  }

  static String getFilteredAttributesById(String nodeId, String filter, boolean disambiguation,
      String wdeIds, boolean source,boolean all) {
    Node nodeAttr = null;
    String ret = null;
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    try {
      Node node = new Node(id);
      if (disambiguation) nodeAttr = adaGdbService.getNodeAttributes(node,
          getWdeIdsFromStringList(wdeIds));
      else nodeAttr = adaGdbService.getNodeAttributes(node, source|(!all));
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    GetAttributesByIdBean bean = new GetAttributesByIdBean();
    bean.initAttrsList();
    if (nodeAttr.getAttributes() != null) {
      for (NodeAttribute nodeAttribute : nodeAttr.getAttributes()) {
        String key = nodeAttribute.getKey();
        if (!key.equals(filter)) continue;
        List<AttrValueInfo> valueInfos = null;
        if(!all){
          if(nodeAttribute.getValues().size() == 1){
            valueInfos = nodeAttribute.getValues();
          }
          else {
            valueInfos = new ArrayList<AttrValueInfo>(1);
            Iterator<AttrValueInfo> iter = nodeAttribute.getValues().iterator();
            AttrValueInfo valueInfo = iter.next();
            int maxTS = WDEUtil.getLatestTimeStamp(valueInfo.getWdeRefs());
            while(iter.hasNext()){
              AttrValueInfo valueInfo2 = iter.next();
              if(maxTS <  WDEUtil.getLatestTimeStamp(valueInfo2.getWdeRefs())){
                maxTS =  WDEUtil.getLatestTimeStamp(valueInfo2.getWdeRefs());
                valueInfo = valueInfo2;
              }
            }
            if(!source)
              valueInfo = new AttrValueInfo(valueInfo.getValue(),null);
            valueInfos.add(valueInfo);
          }
        }
        else valueInfos = nodeAttribute.getValues();
        for (AttrValueInfo valueInfo : valueInfos) {
          ict.ada.gdb.rest.beans.model.NodeAttribute attr = new ict.ada.gdb.rest.beans.model.NodeAttribute();
          attr.setKey(key);
          attr.setValue(valueInfo.getValue());
          attr.setCount(valueInfo.getWdeRefCount());
          bean.addAttrsList(attr);
        }
      }
    }
    bean.setErrorCode("success");
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  static String getAttributeSourceById(String nodeId, String type, String value) {
    String ret = null;
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    if (type == null || value == null) { return generateErrorCodeJson("type or value is null"); }
    AttrValueInfo valueInfo = new AttrValueInfo(value, 0);
    List<AttrValueInfo> values = new ArrayList<AttrValueInfo>(1);
    values.add(valueInfo);
    NodeAttribute nodeAtt = new NodeAttribute(type, values);
    List<NodeAttribute> attributes = new ArrayList<NodeAttribute>(1);
    attributes.add(nodeAtt);
    List<NodeAttribute> result = null;
    try {
      Node node = new Node(id);
      result = adaGdbService.getNodeAttrWdeRefs(node, attributes);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    GetAttributeSourceByIdBean bean = new GetAttributeSourceByIdBean();
    bean.initWderefs();
    bean.setErrorCode("success");
    List<WdeRef> wdeRefs = null;
    NodeAttribute nodeAttribute = null;
    if (result != null) nodeAttribute = result.get(0);
    if (!(nodeAttribute == null)) {
      wdeRefs = nodeAttribute.getValues().get(0).getWdeRefs();
    }
    if (!(wdeRefs == null)) {
      Iterator<WdeRef> iterWdeRefs = wdeRefs.iterator();
      while (iterWdeRefs.hasNext()) {
        WdeRef wdeRef = iterWdeRefs.next();
        //System.out.println(NodeIdConveter.toString(wdeRef.getWdeId()));
        String wdeChannelType = WDEIdConverter.getChannel(wdeRef.getWdeId());
        ict.ada.gdb.rest.beans.model.WdeRef wdeRefBean = new ict.ada.gdb.rest.beans.model.WdeRef();
        wdeRefBean.setWdeid(NodeIdConveter.toString(wdeRef.getWdeId()));
        wdeRefBean.setOffset(wdeRef.getOffset());
        wdeRefBean.setType(wdeChannelType);
        wdeRefBean.setLength(wdeRef.getLength());
        bean.appendWderefs(wdeRefBean);
      }
    }
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  static String getNodeActionInfo(String nodeId, long start, long end) {
    byte[] id = null;
    TimeRange timeRange = TimeRange.ANY_TIME;
    try {
      if (start < 0 && end < 0) timeRange = TimeRange.ANY_TIME;
      else if (start < 0) timeRange = new TimeRange(0, end);
      else timeRange = new TimeRange(start, end);
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    Node node = new Node(id);

    try {
      List<Pair<Integer, NodeAttribute>> result = adaGdbService.getNodeActionAttrWdeRefs(node,null,timeRange);
      GetNodeActionInfoBean bean = new GetNodeActionInfoBean(result);
      return PojoMapper.toJson(bean, true);
    } catch (GdbException e) {
      return generateErrorCodeJson(e.getMessage());
    }
  }

  static String getAttributeSourceById1(String nodeId, String type, String value) {
    String ret = null;
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    if (type == null || value == null) { return generateErrorCodeJson("type or value is null"); }
    AttrValueInfo valueInfo = new AttrValueInfo(value, 0);
    List<AttrValueInfo> values = new ArrayList<AttrValueInfo>(1);
    values.add(valueInfo);
    NodeAttribute nodeAtt = new NodeAttribute(type, values);
    List<NodeAttribute> attributes = new ArrayList<NodeAttribute>(1);
    attributes.add(nodeAtt);
    List<NodeAttribute> result = null;
    try {
      Node node = new Node(id);
      result = adaGdbService.getNodeAttrWdeRefs(node, attributes);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    GetWdeRefDetailBean bean = new GetWdeRefDetailBean();
    List<WdeRef> wdeRefs = null;
    NodeAttribute nodeAttribute = null;
    if (result != null) nodeAttribute = result.get(0);
    if (!(nodeAttribute == null)) {
      wdeRefs = nodeAttribute.getValues().get(0).getWdeRefs();
    }
    if (!(wdeRefs == null)) {
      try {
        bean.setWderefs(InternalWdeService.getDetails1(wdeRefs));
        bean.setTotal(wdeRefs.size());
      } catch (Exception e) {
        return generateErrorCodeJson("Get Wde Detail fail: " + e.getMessage());
      }
    }
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

 
  
  
  static String getNodeActionRefInfo(String nodeId,String value, long start, long end) {
	    byte[] id = null;
	    TimeRange timeRange = TimeRange.ANY_TIME;
	    try {
	      if (start < 0 && end < 0) timeRange = TimeRange.ANY_TIME;
	      else if (start < 0) timeRange = new TimeRange(0, end);
	      else timeRange = new TimeRange(start, end);
	      id = NodeIdConveter.checkAndtoBytes(nodeId);
	    } catch (Exception e) {
	      return generateErrorCodeJson(e.getMessage());
	    }
	    Node node = new Node(id);

	    try {
	      List<Pair<Integer, NodeAttribute>> result = adaGdbService.getNodeActionAttrWdeRefs(node,value,timeRange);
	      GetNodeActionRefInfoBean bean = new GetNodeActionRefInfoBean(result);
	      return PojoMapper.toJson(bean, true);
	    } catch (GdbException e) {
	      return generateErrorCodeJson(e.getMessage());
	    }
	  }
  
  private static List<byte[]> getWdeIdsFromStringList(String wdeIdString) {
    if (wdeIdString == null) return Collections.emptyList();
    String[] wdeIds = wdeIdString.split(",");
    // 应该验证wdeid格式正确性吗？
    List<byte[]> wdeIds1 = new ArrayList<byte[]>(wdeIds.length);
    for (String wdeId : wdeIds)
      wdeIds1.add(StringUtils.hexStringToByte(wdeId));
    return wdeIds1;
  }
  
  

  /**
   * type 不是完整的nodeType 仅仅是Attribute channel 看怎么用了?
   * */
  static String getNodeRelById(String nodeId, String channelName, String type, boolean relInfo,
      int start, int len, long st, long et, String fnode, boolean disambiguation, String wdeIds,
      String relations,boolean staticsticB) {
    String ret = null;
    byte[] id = null;
    byte[] fnodeId = null;
    Attribute attribute = null;
    Channel channel = null;
   boolean getweight= relInfo || staticsticB;
    try {
      if (channelName == null) channel = Channel.ANY;
      else channel = NodeTypeMapper.getChannel(channelName);
      if (type == null) attribute = Attribute.ANY;
      else attribute = NodeTypeMapper.getAttribute(type);
    } catch (Exception e) {
      e.printStackTrace();
      return generateErrorCodeJson(e.getMessage());
    }
    RelationGraph relationGraph = null;
    RelationGraph queryrelationGraph = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
      if (!fnode.equals("None")) fnodeId = NodeIdConveter.checkAndtoBytes(fnode);
    } catch (Exception e) {
      e.printStackTrace();
      return generateErrorCodeJson(e.getMessage());

    }
    if (st == -1) st = Long.MIN_VALUE;
    if (et == -1) et = Long.MAX_VALUE;
    HashSet<RelationType> relationTypes = new HashSet<RelationType>();
    if (!relations.equals("all")) for (String relation : relations.split(",")) {
      relationTypes.add(RelationType.getType(relation));
    }
    TimeRange tr = new TimeRange(st, et);
    try {
      queryrelationGraph = getNodeRelByIdInter(new Node(id), channel, attribute, getweight, tr,
          disambiguation, getWdeIdsFromStringList(wdeIds), relationTypes);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    } catch (Exception e1) {
      e1.printStackTrace();
      return generateErrorCodeJson(e1.getMessage());
    }
    if (fnodeId != null) {
      Node fNode = new Node(fnodeId);
      RelationGraph relationGraphtmp = new RelationGraph(queryrelationGraph.getCenterNode());
      List<Edge> edges = new ArrayList<Edge>();
      for (Node node : queryrelationGraph.getOuterNodes())
        edges.add(new Edge(node, fNode));
      Pair<List<Edge>, List<Edge>> resultEdges = null;
      List<Node> existEdges = new ArrayList<Node>();
      try {
        resultEdges = adaGdbService.getExistEdges(edges);
        for (Edge edge : resultEdges.getFirst())
          existEdges.add(edge.getHead());
        edges.clear();
        for (Edge edge : resultEdges.getSecond())
          edges.add(new Edge(fNode, edge.getHead()));
        resultEdges = adaGdbService.getExistEdges(edges);// 考虑了单向边可能行！！！
        for (Edge edge : resultEdges.getFirst())
          existEdges.add(edge.getTail());
      } catch (GdbException e) {
        return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
      }
      for (Node tail : existEdges) {
        relationGraphtmp.addCenterEdge(new Edge(
            new Node(queryrelationGraph.getCenterNode().getId()), tail));
      }
      queryrelationGraph = relationGraphtmp;
    }
    Set<String> totalRelationTypes = null;
    List<NodeStatistic> nodeStatistic=null;
    if(staticsticB){
      totalRelationTypes = new HashSet<String>();
      Map<NodeType,List<Edge>> statisticMap  = new HashMap<NodeType,List<Edge>>();
      for(Edge edge :queryrelationGraph.getCenterEdges()){
        List<Edge> edges = statisticMap.get(edge.getTailNodeType());
        if(edges == null){ 
          edges = new ArrayList<Edge>();
          statisticMap.put(edge.getTailNodeType(), edges);
        }
        edges.add(edge);
      }
      List<Node> nodes = new ArrayList<Node>();
      for(Entry<NodeType,List<Edge>>e:statisticMap.entrySet()){
        int count = 0;
        for(Edge edge : e.getValue()){
          nodes.add(edge.getTail());
          count++;
          if(count == 10) break;
        }
      }
      try {
        addNodeNameForNodes(nodes);
      } catch (GdbException e1) {
        return generateErrorCodeJson("GdbException happens in query: " + e1.getMessage());
      }
      nodeStatistic = new ArrayList<NodeStatistic>(statisticMap.size());
      for(Entry<NodeType,List<Edge>>e:statisticMap.entrySet()){
        nodeStatistic.add(new NodeStatistic(e.getKey(),e.getValue()));
      }
      for(NodeStatistic statistic :nodeStatistic )
        for(NodeStatistic.Relation rel : statistic.getRelationStatistic())
        totalRelationTypes.add(rel.getRelationType());
    }
    
    else if(relInfo){
    totalRelationTypes = new HashSet<String>();
    for (Edge edge : queryrelationGraph.getCenterEdges()) {
      for (Relation relation : edge.getRelations())
        totalRelationTypes.add(relation.getType().getStringForm());
    }
    }
    if (queryrelationGraph == null) return null;

    relationGraph = new RelationGraph(queryrelationGraph.getCenterNode());
    if (queryrelationGraph.getOuterNodes().size() >= start) {
      Iterator<Edge> iter = queryrelationGraph.getCenterEdges().iterator();
      int count = 0;
      while (count < start) {
        iter.next();
        count++;
      }
      count = 0;
      while (count < len && iter.hasNext()) {
        relationGraph.addCenterEdge(iter.next());
        count++;
      }
    }

    try {
      addNodeNameForRelationGraph(relationGraph);
    } catch (GdbException e1) {
      return generateErrorCodeJson("GdbException happens in query: " + e1.getMessage());

    }
    GetRelationByNodeIdBean bean = new GetRelationByNodeIdBean();
    bean.addRelationGraph(relationGraph);
    bean.setRelationTypeList(totalRelationTypes);
    bean.setTotal(queryrelationGraph.getCenterEdges().size());
    bean.setNodeStatistic(nodeStatistic);
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getRelationTimeLineByNodeId(String nodeId) {
    String ret = null;
    RelationGraph relationGraph = null;
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    try {
      relationGraph = getNodeRelByIdInter(new Node(id), Channel.ANY, Attribute.TIME, false);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    for (Edge edge : relationGraph.getCenterEdges()) {
      List<Pair<Integer, List<Relation>>> result = null;
      try {
        result = adaGdbService.getEdgeRelations(edge, null, null, false);
        if (result != null) {
          List<Relation> relations = result.get(0).getSecond();
          for (Relation relation : relations)
            edge.addRelation(relation);
        }
      } catch (GdbException e) {
        // TODO Auto-generated catch block
        return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
      }
    }
    GetRelationTimeLineByNodeIdBean bean = new GetRelationTimeLineByNodeIdBean(relationGraph);
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getRelationInferData(String nodeId, String rule) {
    OpenioRuleBean rulebean = null;
    List<RelationGraph> result = null;
    String ret = null;
    try {
      rulebean = (OpenioRuleBean) PojoMapper.fromJson(rule, OpenioRuleBean.class);
    } catch (Exception e) {
      return InternalNodeService.generateErrorCodeJson("something wrong with the json[  " + rule
          + "   ] : " + e.getMessage());
    }
    // PrintStream ps=null;
    // PrintStream ps2=null;
    // try {
    // ps = new PrintStream(new File("/ada/sys/gdb-rest-dev/log/logfile"));
    // ps=new PrintStream(new File("/home/wangqianbo/test/test"));
    // } catch (FileNotFoundException e2) {
    // e2.printStackTrace();
    // }
    long starttime = Timer.now();
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return InternalNodeService.generateErrorCodeJson(e.getMessage());
    }
    Node startnode = new Node(id);
    try {
      result = getRelationInferGraphs(startnode, rulebean);
    } catch (Exception e) {
      return InternalNodeService.generateErrorCodeJson("GdbException happens in query: " + e);
    }
    // ps.println("InternalNodeService:gen result   in " + Timer.msSince(starttime) + "ms " );
    // TODO result中可能有些点是重复的, 重复率应该不高,这么做提升不大,不如取所有点的name.
    starttime = Timer.now();
    HashMap<NodeType, HashSet<ByteArray>> nodeIdsMap = new HashMap<NodeType, HashSet<ByteArray>>();
    HashSet<ByteArray> nodeIds = new HashSet<ByteArray>();
    nodeIds.add(new ByteArray(result.get(0).getCenterNode().getId()));
    nodeIdsMap.put(result.get(0).getCenterNode().getType(), nodeIds);
    ByteArray gnodeId = null;
    for (RelationGraph relgraph : result) {
      for (Node node : relgraph.getOuterNodes()) {
        gnodeId = new ByteArray(node.getId());
        if (nodeIdsMap.get(node.getType()) == null) {
          nodeIds = new HashSet<ByteArray>();
          nodeIds.add(gnodeId);
          nodeIdsMap.put(node.getType(), nodeIds);
        } else {
          nodeIdsMap.get(node.getType()).add(gnodeId);
        }
      }
    }
    HashMap<ByteArray, Pair<String, List<String>>> nodeIdNameMap = new HashMap<ByteArray, Pair<String, List<String>>>();
    for (Map.Entry<NodeType, HashSet<ByteArray>> e : nodeIdsMap.entrySet()) {
      ArrayList<byte[]> ids = new ArrayList<byte[]>();
      for (ByteArray bt : e.getValue())
        ids.add(bt.getBytes());
      List<Pair<String, List<String>>> names = null;
      try {
        names = adaGdbService.getNodeNameAndSnameByIdBatched(ids);
      } catch (GdbException e1) {
        return InternalNodeService.generateErrorCodeJson("GdbException happens in query: "
            + e1.getMessage());
      }
      Iterator<ByteArray> idsIter = e.getValue().iterator();
      Iterator<Pair<String, List<String>>> namesIter = names.iterator();
      while (idsIter.hasNext()) {
        nodeIdNameMap.put(idsIter.next(), namesIter.next());
      }
    }
    // ps.println("InternalNodeService:gen nodeIdNameMap   in " + Timer.msSince(starttime) + "ms "
    // );
    starttime = Timer.now();
    GetRelationByNodeIdBean bean = new GetRelationByNodeIdBean();
    for (RelationGraph rel : result)
      bean.addRelationGraph(rel, nodeIdNameMap);
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  private static List<RelationGraph> getRelationInferGraphs(Node startnode,
      final OpenioRuleBean bean) throws GdbException, InterruptedException, ExecutionException { // 现在只能处理两层结构,
                                                                                                 // 且考虑多线程实现，在一般情况下，算法比较复杂。
    List<RelationGraph> result = new LinkedList<RelationGraph>();
    RelQuerySpec spec = getRelQuerySpec(startnode, bean.getRule().get(0));
    RelationGraph startRes = adaGdbService.queryRelationGraph(spec);
    result.add(startRes);
    List<RelQuerySpec> specList = new ArrayList<RelQuerySpec>();
    List<Edge> edgeTodel = new ArrayList<Edge>();
    int edgecount = startRes.getCenterEdges().size();
    Iterator<Edge> iter = startRes.getCenterEdges().iterator();// 最多保留20个第一层节点
    System.out.println(startRes.getOuterNodes().size());
    for (int i = edgecount; i > 20; i--)
      edgeTodel.add(iter.next());
    for (Edge edge : edgeTodel)
      startRes.removeCenterEdgeAndRelatedOuterNode(edge);
    System.out.println(startRes.getOuterNodes().size());
    for (Node node : startRes.getOuterNodes())
      specList.add(getRelQuerySpec(node, bean.getRule().get(1)));
    List<RelationGraph> subResult = adaGdbService.queryRelationGraphsInParallel(specList, 1);
    for (RelationGraph rel : subResult) {
      ArrayList<Edge> edgeToDel = new ArrayList<Edge>(); // 删除回边，由于很多关系都是双向的，因此要删除那些回边。
      for (Edge edge1 : rel.getCenterEdges()) {
        if (Bytes.equals(edge1.getTail().getId(), startnode.getId())) {
          edgeToDel.add(edge1);
        }
      }
      for (Edge edge2 : edgeToDel)
        rel.removeCenterEdgeAndRelatedOuterNode(edge2);
      if (rel.getOuterNodes().size() == 0) {
        Edge edge = null;
        // System.out.println("****************"+rel.getCenterNode().getName());
        for (Edge edge1 : startRes.getCenterEdges()) {
          if (Arrays.equals(edge1.getTail().getId(), rel.getCenterNode().getId())) {
            edge = edge1;
            break;
          }
        }
        startRes.removeCenterEdgeAndRelatedOuterNode(edge);
      } else result.add(rel);
    }
    return result;
  }

  private static RelQuerySpec getRelQuerySpec(Node node, Regulation regulation) {
    System.out.println(regulation.getNode().get(0));
    System.out.println(regulation.getRelation());
    RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(node);
    relbuilder.attribute(NodeTypeMapper.getAttribute(regulation.getNode().get(0)));// NodeType现在只支持一种
    relbuilder.resultSize(20);
    for (String relation : regulation.getRelation())
      relbuilder.relType(RelationType.getType(relation));
    RelQuerySpec spec = relbuilder.build();
    return spec;
  }

  
  public static String getReleventNodes(String nodeIdStr, List<String> types, int len) {
    String json = null;
    byte[] nodeId = null;
    HashSet<Attribute> nodeTypeSet = new HashSet<Attribute>(types.size());
    try {
       nodeId = NodeIdConveter.checkAndtoBytes(nodeIdStr);
      for (String type : types) {
        // System.out.println(type);
        if (type.equals("all")) for (Attribute nodeType : Attribute.values()) {
          if ( nodeType != Attribute.ANY) nodeTypeSet.add(nodeType);
        }
        else nodeTypeSet.add(NodeTypeMapper.getAttribute(type));
      }
    } catch (Exception e) {
      return InternalServiceResources.generateErrorCodeJson(e.getMessage());
    }
    Node startNode = new Node(nodeId);
    ReleventNodeSearcher searcher  = new ReleventNodeSearcher(startNode,nodeTypeSet,adaGdbService , len,30);
    try {
      searcher.genSimilarEvents();
      InternalNodeService.addNodeNameForNodes(searcher.getElementsMap().values());
    } catch (GdbException e) {
      return InternalServiceResources.generateErrorCodeJson("wrong query " + e.getMessage());
    }
    List<Triplet<Node, List<Node>, Integer>> result = searcher.getSimilarNodes();
    List<Node> nodes = new ArrayList<Node>(len);
    for (Triplet<Node, List<Node>, Integer> triplet : result)
      nodes.add(triplet.getValue0());
    try {
      InternalNodeService.addNodeNameForNodes(nodes);
    } catch (GdbException e) {
      return InternalServiceResources.generateErrorCodeJson("wrong query " + e.getMessage());
    }
    GetReleventNodesBean bean = new GetReleventNodesBean(len);
    for (Triplet<Node, List<Node>, Integer> similarEvent : result) {
             bean.addNode(similarEvent.getValue0(), similarEvent.getValue1(),
                 similarEvent.getValue2());
    }
    json = PojoMapper.toJson(bean, true);
    return json;

  }
  
  static String getNodeNameById(String nodeId) {
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    Node resultNode = null;
    String ret = null;
    try {
      resultNode = adaGdbService.getNodeNameAndSnameById(id);
    } catch (Exception e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    checkNode(resultNode);
    GetNodeNameByIdBean bean = new GetNodeNameByIdBean(resultNode);
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getNodeHierarchyById(String nodeId,boolean sameType) {
    String ret = null;
    RelationGraph relationGraph = null;
    byte[] id = null;
    Collection<String> relTypeArray = InternalServiceResources.getAncestryType();
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    List<RelationType> relTypeList = new LinkedList<RelationType>();
    for (String relType : relTypeArray)
      relTypeList.add(RelationType.getType(relType));
    try {
      Node node = new Node(id);
      if(sameType)
    	  relationGraph = getNodeRelByIdInter(node, node.getType().getChannel(), node.getType()
          .getAttribute(), false, Integer.MAX_VALUE, relTypeList);
      else 
    	  relationGraph = getNodeRelByIdInter(node, node.getType().getChannel(), NodeType.Attribute.ANY, false, Integer.MAX_VALUE, relTypeList);
      // addNodeNameForRelationGraph(relationGraph);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    Map<String, String> reMap = new HashMap<String, String>();
    reMap.put("errorCode", "success");
    if (relationGraph.getOuterNodes().isEmpty()) {
      reMap.put("result", "false");
    } else reMap.put("result", "true");

    ret = PojoMapper.toJson(reMap, true); // assert no
    // exception
    // happen
    return ret;
  }

  public static String getNodeSuperiorById(String nodeId, String addtion,boolean sameType) {
    String ret = null;
    RelationGraph relationGraph = null;
    byte[] id = null;
    Collection<String> relTypeArray = InternalServiceResources.getSuperiorType();
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    List<RelationType> relTypeList = new LinkedList<RelationType>();
    for (String relType : relTypeArray)
      relTypeList.add(RelationType.getType(relType));
    try {
      Node node = new Node(id);
      if(sameType)
      relationGraph = getNodeRelByIdInter(node, node.getType().getChannel(), node.getType()
          .getAttribute(), false, Integer.MAX_VALUE, relTypeList);
      else
    	  relationGraph = getNodeRelByIdInter(node, node.getType().getChannel(), NodeType.Attribute.ANY, false, Integer.MAX_VALUE, relTypeList);
      addNodeNameForRelationGraph(relationGraph);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    GetNodeHierarchyByIdBean bean = new GetNodeHierarchyByIdBean();
    bean.setAddtion(addtion);
    bean.addRelationGraph(relationGraph);
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getNodeSubordinateById(String nodeId, String addtion,boolean sameType) {
    String ret = null;
    RelationGraph relationGraph = null;
    byte[] id = null;
    Collection<String> relTypeArray = InternalServiceResources.getSubordinateType();
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    List<RelationType> relTypeList = new LinkedList<RelationType>();
    for (String relType : relTypeArray)
      relTypeList.add(RelationType.getType(relType));
    try {
      Node node = new Node(id);
      if(sameType)
      relationGraph = getNodeRelByIdInter(node, node.getType().getChannel(), node.getType()
          .getAttribute(), false, Integer.MAX_VALUE, relTypeList);
      else
    	  relationGraph = getNodeRelByIdInter(node, node.getType().getChannel(), NodeType.Attribute.ANY, false, Integer.MAX_VALUE, relTypeList);
      addNodeNameForRelationGraph(relationGraph);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    GetNodeHierarchyByIdBean bean = new GetNodeHierarchyByIdBean();
    bean.setAddtion(addtion);
    bean.addRelationGraph(relationGraph);
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getNodeRelNodeClusterById(String centerNodeId, String type, int mincluster,
      int maxcluster, int maxnode, int weight) {
    String ret = null;
    RelationGraph relationGraph = null;
    byte[] id = null;
    Attribute attribute = null;

    try {
      if (type.equals("all")) attribute = Attribute.ANY;
      else attribute = NodeTypeMapper.getAttribute(type);
      id = NodeIdConveter.checkAndtoBytes(centerNodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    List<RelationType> relTypeList = new LinkedList<RelationType>();
    relTypeList.add(RelationType.getType("共现"));// TODO 此处还是只是限制共现?
    try {
      relationGraph = getNodeRelByIdInter(new Node(id), Channel.ANY, attribute, false, maxnode,
          relTypeList);
      RelationGraph relationGraphtmp = new RelationGraph(relationGraph.getCenterNode());
      for (Edge edge : relationGraph.getCenterEdges())
        if (edge.getEdgeWeight() >= weight) relationGraphtmp.addCenterEdge(edge);
      relationGraph = relationGraphtmp;
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }

    Map<ByteArray, Collection<ByteArray>> outerNodeWithWdeIds = null;
    try {
      outerNodeWithWdeIds = getOuterNodeWithWdeIds(relationGraph, weight);
      RelationGraph relationGraphtmp = new RelationGraph(relationGraph.getCenterNode());
      for (Edge edge : relationGraph.getCenterEdges())
        if (edge.getEdgeWeight() >= weight) relationGraphtmp.addCenterEdge(edge);
      relationGraph = relationGraphtmp;
    } catch (GdbException e1) {
      return generateErrorCodeJson("GdbException happens in query: " + e1.getMessage());
    }
    // PrintStream ps = new PrintStream(new File("/home/wangqianbo/test/test"));
    // for(Entry<ByteArray, Collection<ByteArray>> e:outerNodeWithWdeIds.entrySet()){
    // ps.print(NodeIdConveter.toString(e.getKey().getBytes()));
    // ps.print("   [");
    // for(ByteArray bt:e.getValue()){
    // ps.print(NodeIdConveter.toString(bt.getBytes()));
    // ps.print(",");
    // }
    // ps.println("]");
    // }
    // ps.println(PojoMapper.toJson(outerNodeWithWdeIds, true));
    long start = System.currentTimeMillis();
    CommunityDiscovery cd = new CommunityDiscovery();
    cd.setMinCommunityNumber(mincluster);
    cd.discoverCommunity(outerNodeWithWdeIds);
    List<List<ByteArray>> communities = cd.getCommunities();
    long end = System.currentTimeMillis();
    System.out.println(end - start);
    GetNodeRelNodeClusterByIdBean bean = new GetNodeRelNodeClusterByIdBean();
    List<List<Node>> communitiesNode = new ArrayList<List<Node>>();
    HashMap<ByteArray, Node> idNodeMap = new HashMap<ByteArray, Node>();
    for (Node node : relationGraph.getOuterNodes()) {
      idNodeMap.put(new ByteArray(node.getId()), node);
    }
    try {
      addNodeNameForNodes(idNodeMap.values());
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    int count = 0;
    for (List<ByteArray> cluster : communities) {
      count++;
      if (count > maxcluster) break;
      List<Node> nodeCluster = new ArrayList<Node>();
      for (ByteArray nodeId : cluster) {
        nodeCluster.add(idNodeMap.get(nodeId));
      }
      communitiesNode.add(nodeCluster);

    }
    bean.genClusters(communitiesNode,
        NodeIdConveter.toString(relationGraph.getCenterNode().getId()));
    bean.genEdgeList(relationGraph.getCenterEdges());
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getPathBetNodes(String start, String end, int degree, int max, String type) {
    byte[] startId = null;
    byte[] endId = null;
    PathGraph pathgraph = null;
    String ret = null;
    try {
      startId = NodeIdConveter.checkAndtoBytes(start);
      endId = NodeIdConveter.checkAndtoBytes(end);
      if (Bytes.equals(startId, endId)) throw new IllegalArgumentException(
          "path start and end can't be the same");
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    NodeType startType = NodeType.getType(startId[0], startId[1]);
    NodeType endType = NodeType.getType(endId[0], endId[1]);
    Attribute attribute = null;
    if (type != null) {
      if (type.equals("all")) attribute = Attribute.ANY;
      else if (type.equals("NOEXISTS")) {
        if (!startType.equals(endType)) return generateErrorCodeJson("startNode and endNode have different NodeType");
        attribute = startType.getAttribute();
      } else {
        attribute = NodeTypeMapper.getAttribute(type);
        if (!startType.equals(endType) || startType.getAttribute() != attribute) return generateErrorCodeJson("startNode , endNode queryType  have different NodeType");

      }
    }

    if (attribute == null) attribute = startType.getAttribute();
    // if (!startType.equals(endType)) return
    // generateErrorCodeJson("startNode and endNode have different NodeType");
    // PrintStream ps=null;
    // PrintStream ps2=null;
    // try {
    // ps = new PrintStream(new File("/ada/sys/gdb-rest-dev/log/logfile"));
    // ps=new PrintStream(new File("/home/wangqianbo/test/test"));
    // } catch (FileNotFoundException e2) {
    //
    // / e2.printStackTrace();
    // }
    HashSet<Node> nodes = new HashSet<Node>();
    HashSet<ByteArray> edges = new HashSet<ByteArray>();
    String url = InternalWdeService.channelService.getProperty(String.valueOf(startType
        .getIntegerForm()));
    url = null;// 现在统一起来。
    if (false) {
      StringBuilder fullurl = new StringBuilder();
      fullurl.append(url).append("cmd=findallPaths&start=").append(start).append("&end=")
          .append(end).append("&depth=").append(degree).append("&limits=").append(max);
      String pathJson = InternalServiceResources.downloadHtml(fullurl.toString());
      System.out.println(pathJson);
      try {
        NodesPathBean nodesPathBean = (NodesPathBean) PojoMapper.fromJson(pathJson,
            NodesPathBean.class);
        if (!nodesPathBean.getErrorCode().equals("success")) return pathJson;
        for (NodesPathBean.Path path : nodesPathBean.getPaths()) {
          int len = path.getPath().size();
          for (int i = 0; i < len - 1; i++) {
            nodes.add(new Node(EdgeIdConverter.checkAndtoBytes(path.getPath().get(i).getNodeId())));
            byte[] edgeid = EdgeIdConverter.checkAndtoBytes(path.getPath().get(i).getNodeId()
                + path.getPath().get(i + 1).getNodeId());
            ByteArray bt = new ByteArray(edgeid);
            edges.add(bt);
          }
        }
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
    }

    else {
      long starttime = Timer.now();
      PathQuerySpec.PathQuerySpecBuilder builder = new PathQuerySpec.PathQuerySpecBuilder(startId,
          endId);
      builder.requiredAttribute(attribute);
      builder.maxPathLength(degree - 1);// 在这个方法中的参数指的是跳数,而degree意思指的是路径中点的长度.
      try {
        pathgraph = adaGdbService.queryPathGraph(builder.build());
      } catch (GdbException e) {
        return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
      }
      // ps.println("InternalNodeService: Get pathgraph in " + Timer.msSince(starttime) + "ms " );
      starttime = Timer.now();
      DfsPathGraph d = new DfsPathGraph(max, degree, pathgraph);
      d.maindfs();
      // ps.println("InternalNodeService: gen paths in " + Timer.msSince(starttime) + "ms " );
      starttime = Timer.now();
      for (Path path : d.getMinheap().getHeap()) {
        if (path == null) continue;
        int len = path.getNodeList().size();
        for (int i = 0; i < len - 1; i++) {
          nodes.add(path.getNodeList().get(i));
          byte[] edgeid = Bytes.add(path.getNodeList().get(i).getId(), path.getNodeList()
              .get(i + 1).getId());
          ByteArray bt = new ByteArray(edgeid);
          edges.add(bt);
        }
      }
      // ps.println("InternalNodeService: get prepare  in " + Timer.msSince(starttime) + "ms " );
    }
    long starttime1 = Timer.now();
    nodes.add(new Node(endId));
    nodes.add(new Node(startId));
    Map<ByteArray, Node> idtoNode = new HashMap<ByteArray, Node>();
    for (Node node : nodes)
      idtoNode.put(new ByteArray(node.getId()), node);
    try {
      addNodeNameForNodes(idtoNode.values());
    } catch (GdbException e1) {
      return generateErrorCodeJson("GdbException happens in query: " + e1.getMessage());
    }

    // ps.println("InternalNodeService: get names  in " + Timer.msSince(starttime1) + "ms " );
    starttime1 = Timer.now();
    GetPathBetNodesBean bean = new GetPathBetNodesBean();
    for (ByteArray edgeid : edges) {
      byte[] edge_id = edgeid.getBytes();
      byte[] end_id = Arrays.copyOfRange(edge_id, edge_id.length / 2, edge_id.length);
      Node endNode = idtoNode.get(new ByteArray(end_id));
      bean.addEdge(edgeid, endNode);
    }
    bean.addNode(idtoNode.get(new ByteArray(startId)));
    // ps.println("InternalNodeService: gen  bean  in " + Timer.msSince(starttime1) + "ms " );
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getPathBetNodes2(String start, String end, int degree, int max, String type) {
    byte[] startId = null;
    byte[] endId = null;
    PathGraph pathgraph = null;
    String ret = null;
    try {
      startId = NodeIdConveter.checkAndtoBytes(start);
      endId = NodeIdConveter.checkAndtoBytes(end);
      if (Bytes.equals(startId, endId)) throw new IllegalArgumentException(
          "path start and end can't be the same");
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    NodeType startType = NodeType.getType(startId[0], startId[1]);
    NodeType endType = NodeType.getType(endId[0], endId[1]);
    Attribute attribute = null;
    if (type != null) {
      if (type.equals("all")) attribute = Attribute.ANY;
      else if (type.equals("NOEXISTS")) {
        if (!startType.equals(endType)) return generateErrorCodeJson("startNode and endNode have different NodeType");
        attribute = startType.getAttribute();
      } else {
        attribute = NodeTypeMapper.getAttribute(type);
        if (!startType.equals(endType) || startType.getAttribute() != attribute) return generateErrorCodeJson("startNode , endNode queryType  have different NodeType");

      }
    }

    if (attribute == null) attribute = startType.getAttribute();
    // PrintStream ps=null;
    // PrintStream ps2=null;
    // try {
    // ps = new PrintStream(new File("/ada/sys/gdb-rest-dev/log/logfile"));
    // ps=new PrintStream(new File("/home/wangqianbo/test/test"));
    // } catch (FileNotFoundException e2) {
    // TODO Auto-generated catch block
    // / e2.printStackTrace();
    // }
    HashSet<Node> nodes = new HashSet<Node>();
    HashSet<ByteArray> edges = new HashSet<ByteArray>();

    long starttime = Timer.now();
    PathQuerySpec.PathQuerySpecBuilder builder = new PathQuerySpec.PathQuerySpecBuilder(startId,
        endId);
    builder.maxPathLength(degree - 1);
    builder.requiredAttribute(attribute);
    try {
      pathgraph = adaGdbService.queryPathGraph(builder.build());
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    // ps.println("InternalNodeService: Get pathgraph in " + Timer.msSince(starttime) + "ms " );
    starttime = Timer.now();
    DfsPathGraph d = new DfsPathGraph(max, degree, pathgraph);
    d.maindfs();
    // ps.println("InternalNodeService: gen paths in " + Timer.msSince(starttime) + "ms " );
    starttime = Timer.now();
    for (Path path : d.getMinheap().getHeap()) {
      if (path == null) continue;
      int len = path.getNodeList().size();
      for (int i = 0; i < len - 1; i++) {
        nodes.add(path.getNodeList().get(i));
      }
    }
    // ps.println("InternalNodeService: get prepare  in " + Timer.msSince(starttime) + "ms " );
    long starttime1 = Timer.now();
    nodes.add(pathgraph.getGraphStart());
    nodes.add(pathgraph.getGraphEnd());

    try {
      addNodeNameForNodes(nodes);
    } catch (GdbException e1) {
      return generateErrorCodeJson("GdbException happens in query: " + e1.getMessage());
    }

    // ps.println("InternalNodeService: get names  in " + Timer.msSince(starttime1) + "ms " );
    starttime1 = Timer.now();
    GetPathBetNodes2Bean bean = new GetPathBetNodes2Bean();
    bean.addPaths(d.getMinheap().getHeap());
    bean.addNodes(nodes);
    // bean.addEdges(edges1);
    // ps.println("InternalNodeService: gen  bean  in " + Timer.msSince(starttime1) + "ms " );
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getPathBetNodes3(String start, String end, int degree, int max, String type) {
    byte[] startId = null;
    byte[] endId = null;
    PathGraph pathgraph = null;
    String ret = null;
    try {
      startId = NodeIdConveter.checkAndtoBytes(start);
      endId = NodeIdConveter.checkAndtoBytes(end);
      if (Bytes.equals(startId, endId)) throw new IllegalArgumentException(
          "path start and end can't be the same");
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    NodeType startType = NodeType.getType(startId[0], startId[1]);
    NodeType endType = NodeType.getType(endId[0], endId[1]);
    Attribute attribute = null;
    if (type != null) {
      if (type.equals("all")) attribute = Attribute.ANY;
      else if (type.equals("NOEXISTS")) {
        if (!startType.equals(endType)) return generateErrorCodeJson("startNode and endNode have different NodeType");
        attribute = startType.getAttribute();
      } else {
        attribute = NodeTypeMapper.getAttribute(type);
        if (!startType.equals(endType) || startType.getAttribute() != attribute) return generateErrorCodeJson("startNode , endNode queryType  have different NodeType");

      }
    }

    if (attribute == null) attribute = startType.getAttribute();
    // PrintStream ps=null;
    // PrintStream ps2=null;
    // try {
    // ps = new PrintStream(new File("/ada/sys/gdb-rest-dev/log/logfile"));
    // ps=new PrintStream(new File("/home/wangqianbo/test/test"));
    // } catch (FileNotFoundException e2) {
    // TODO Auto-generated catch block
    // / e2.printStackTrace();
    // }
    HashSet<Node> nodes = new HashSet<Node>();
    HashSet<ByteArray> edges = new HashSet<ByteArray>();

    long starttime = Timer.now();
    PathQuerySpec.PathQuerySpecBuilder builder = new PathQuerySpec.PathQuerySpecBuilder(startId,
        endId);
    builder.maxPathLength(degree - 1);
    builder.requiredAttribute(attribute);
    try {
      pathgraph = adaGdbService.queryPathGraph(builder.build());
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    // ps.println("InternalNodeService: Get pathgraph in " + Timer.msSince(starttime) + "ms " );
    starttime = Timer.now();
    DfsPathGraph d = new DfsPathGraph(max, degree, pathgraph);
    d.maindfs();
    // ps.println("InternalNodeService: gen paths in " + Timer.msSince(starttime) + "ms " );
    starttime = Timer.now();
    List<Edge> edges1 = new ArrayList<Edge>();
    for (Path path : d.getMinheap().getHeap()) {
      if (path == null) continue;
      int len = path.getNodeList().size();
      for (int i = 0; i < len - 1; i++) {
        nodes.add(path.getNodeList().get(i));
        byte[] edgeid = Bytes.add(path.getNodeList().get(i).getId(), path.getNodeList().get(i + 1)
            .getId());
        ByteArray bt = new ByteArray(edgeid);
        if (edges.add(bt)) {
          edges1.add(new Edge(path.getNodeList().get(i), path.getNodeList().get(i + 1)));
        }
      }
    }
    try {
      adaGdbService.getEdgesRelations(edges1, null, TimeRange.ANY_TIME, false);
    } catch (GdbException e2) {
      return generateErrorCodeJson("GdbException happens in query: " + e2.getMessage());
    }
    // ps.println("InternalNodeService: get prepare  in " + Timer.msSince(starttime) + "ms " );
    long starttime1 = Timer.now();
    nodes.add(pathgraph.getGraphStart());
    nodes.add(pathgraph.getGraphEnd());

    try {
      addNodeNameForNodes(nodes);
    } catch (GdbException e1) {
      return generateErrorCodeJson("GdbException happens in query: " + e1.getMessage());
    }

    // ps.println("InternalNodeService: get names  in " + Timer.msSince(starttime1) + "ms " );
    starttime1 = Timer.now();
    GetPathBetNodes2Bean bean = new GetPathBetNodes2Bean();
    bean.addPaths(d.getMinheap().getHeap());
    bean.addNodes(nodes);
    bean.addEdges(edges1);
    // ps.println("InternalNodeService: gen  bean  in " + Timer.msSince(starttime1) + "ms " );
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getCliquesOfNode(String nodeId, String type, int limit, int maxcliques) {
    String ret = null;
    long edge_count = 0;
    RelationGraph relationGraph = null;
    byte[] id = null;
    // 1 构造点直接关系点的邻接矩阵.
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    List<RelationType> relTypeList = new LinkedList<RelationType>();
    // relTypeList.add(RelationType.getType(100001));
    // NodeType nodeType = NodeType.getType(Bytes.head(id, 2));
    Attribute attribute = null;

    try {
      // 1.1 构造查询条件
      attribute = NodeTypeMapper.getAttribute(type);
      relationGraph = getNodeRelByIdInter(new Node(id), Channel.ANY, attribute, false, limit,
          relTypeList);
    } catch (Exception e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    // PrintStream ps=null;
    // PrintStream ps2=null;
    // try {
    // ps = new PrintStream(new File("/ada/sys/gdb-rest-dev/log/logfile"));
    // ps=new PrintStream(new File("/home/wangqianbo/test/test"));
    // } catch (FileNotFoundException e2) {
    //
    // e2.printStackTrace();
    // }
    // 1.2 精简图
    int numOfNode = relationGraph.getOuterNodes().size() + 1;
    System.out.println(numOfNode);
    numOfNode = (numOfNode > limit) ? limit : numOfNode;

    // TODO numOfNode ==0的处理
    Node[] nodes = new Node[numOfNode];
    // 该map 为点=>邻接矩阵坐标
    HashMap<ByteArray, Integer> nodeMap = new HashMap<ByteArray, Integer>();
    boolean[][] graph = new boolean[numOfNode][numOfNode];
    for (int n = 0; n < numOfNode; n++)
      for (int m = n; m < numOfNode; m++)
        graph[n][m] = graph[m][n] = false;
    graph[0][0] = true;
    int i = 1;
    try {
      nodes[0] = adaGdbService.getNodeNameAndSnameById(id);
    } catch (GdbException e1) {
      return generateErrorCodeJson("GdbException happens in query: " + e1.getMessage());
    }
    List<RelQuerySpec> specList = new ArrayList<RelQuerySpec>();
    int count = 0;
    // 1.3 根据点直接关系点初始化图的邻接矩阵
    for (Node node : relationGraph.getOuterNodes()) {
      count++;
      if (count == numOfNode) break;
      nodeMap.put(new ByteArray(node.getId()), count);
      RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(node);
      relbuilder.attribute(attribute);
      // System.out.println(nodeType.getIntegerForm());
      RelQuerySpec spec = relbuilder.build();
      specList.add(spec);
      nodes[i] = node;
      graph[0][i] = true;
      graph[i][i] = true;
      graph[i][0] = true;
      i++;
    }

    // long start = Timer.now();
    List<RelationGraph> relationGraphList = null;
    try {
      relationGraphList = adaGdbService.queryRelationGraphsInParallel(specList,
          specList.size() / 16 + 1);
    } catch (GdbException e1) {
      return generateErrorCodeJson("GdbException happens in query: " + e1.getMessage());
    }
    // ps.println("InternalNodeService: Get relationGraphList in " + Timer.msSince(start) + "ms "
    // +numOfNode+"nodes");
    i = 1;
    // 1.4 构造图,写入边
    boolean[] reserve = new boolean[numOfNode];
    int reserveCount = 1;
    reserve[0] = true;
    // start = Timer.now();
    for (RelationGraph relationGraph1 : relationGraphList) {
      int degree = 0;
      for (Node nodeB : relationGraph1.getOuterNodes()) {
        Integer index = nodeMap.get(new ByteArray(nodeB.getId()));
        if (index != null) {
          edge_count++;
          if (i != index && index != 0) degree++;
          graph[i][index] = graph[index][i] = true;

        }
      }
      if (degree > 0) {
        reserve[i] = true;
        reserveCount++;
      } else reserve[i] = false;
      i++;
    }
    // ps.println("InternalNodeService: build old graph  in " + Timer.msSince(start) + "ms "
    // +numOfNode+"nodes");
    // 1.5 再次精简图,把孤立点去掉.
    boolean reserveGraph[][] = new boolean[reserveCount][reserveCount];
    Node[] nodesReserve = new Node[reserveCount];
    System.out.println(reserveCount);
    int row = 0;
    int column = 0;
    for (int m = 0; m < numOfNode; m++) {
      if (reserve[m]) {
        column = 0;
        nodesReserve[row] = nodes[m];
        for (int n = 0; n < numOfNode; n++)
          if (reserve[n]) {
            reserveGraph[row][column] = graph[m][n];
            column++;
          }
        row++;
      }
    }
    // for(int m=0;m<numOfNode;m++){
    // for(int n=0;n<numOfNode;n++)
    // ps2.print(graph[m][n]);
    // / ps2.println();
    // }
    // ps.println("InternalNodeService: the new graph  has " + reserveCount + "nodes  ==\\"
    // +numOfNode+"nodes");
    // ps.println("InternalNodeService: there are  " + edge_count + " edges  and "
    // +numOfNode+"nodes" +" the  bian/dian = " +
    // (double)edge_count/((numOfNode-1.0)*(numOfNode-1.0)));
    // ps.println("InternalNodeService: build new  graph  in " + Timer.msSince(start) + "ms "
    // +numOfNode+"nodes");
    // long end = System.currentTimeMillis();
    // System.out.println(end-start);
    // start = Timer.now();
    // 2 计算极大团
    MaxClique mc = new MaxClique(reserveGraph, reserveCount, maxcliques);
    mc.genCliques();
    // ps.println("InternalNodeService:there are  " + mc.totalCliques + " cliques  of "
    // +numOfNode+"nodes");
    // ps.println("InternalNodeService:gen all maximal cliques  in " + Timer.msSince(start) + "ms "
    // +numOfNode+"nodes");
    // start = Timer.now();
    boolean[][] graph_clique = new boolean[reserveCount][reserveCount];
    for (int m = 0; m < reserveCount; m++)
      for (int n = 0; n < reserveCount; n++)
        graph_clique[m][n] = false;
    GetCliquesOfNodeBean bean = new GetCliquesOfNodeBean();
    bean.addStatistics(mc.getInfo(),
        (edge_count * 1.0 + numOfNode * 2.0) / (numOfNode * numOfNode), mc.getTotalCliques(),
        numOfNode, (int) (edge_count / 2 + numOfNode - 1));
    for (Clique c : mc.getCliques().getHeap()) {
      if (c == null) continue;
      graph_clique[0][c.getClique()[c.getLength() - 1]] = true;
      for (int m = 0; m < c.getLength() - 1; m++) {
        graph_clique[0][c.getClique()[m]] = true;
        for (int n = m + 1; n < c.getLength(); n++)
          graph_clique[c.getClique()[m]][c.getClique()[n]] = graph_clique[c.getClique()[n]][c
              .getClique()[m]] = true;
      }
    }
    //
    try {
      addNodeNameForNodes(Arrays.asList(nodesReserve));
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    for (int m = 0; m < reserveCount; m++)
      for (int n = m + 1; n < reserveCount; n++) {
        if (graph_clique[m][n]) {
          bean.addEdge(nodesReserve[m].getId(), nodesReserve[n]);
        }
      }
    bean.addNode(nodes[0]);
    // ps.println("InternalNodeService:gen bean   in " + Timer.msSince(start) + "ms " + numOfNode
    // + "nodes");
    // start = Timer.now();
    // GetCliquesOfNodeBean bean = new GetCliquesOfNodeBean(nodes, mc.getCliques());

    ret = PojoMapper.toJson(bean, true);
    // ps.println("InternalNodeService:gen json data  in " + Timer.msSince(start) + "ms "
    // +numOfNode+"nodes");
    return ret;
  }

  /**
   * @param nodeId
   * @param channel
   * @param maxcommunity
   *          最多的社区返回数，
   * @param maxnode
   *          最多点个数。由genAdjGraphForNode限制。
   * @return
   */
  public static String getNodeClusterC(String nodeId, String channel, int maxcommunity, int maxnode) {
    long start = System.currentTimeMillis();
    String ret = null;
    Map<String, Map<String, Double>> graph = null;
    try {// 返回算法要求的邻接图
      graph = genAdjGraphForNode(nodeId, channel, maxnode);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    catch(Exception e){
      e.printStackTrace();
      if(graph == null)
        System.out.println("graph == null");
      System.out.println(e.getStackTrace());
    }
    start = System.currentTimeMillis();
    ict.software.ada.community.CommunitySearch commSearch = new ModularityHierarchCommunitySearch();
    List<List<String>> clusters = commSearch.search(graph);
    // nodeid => Node 映射,用于获取nodeName
    HashMap<String, Node> id2NodeMap = new HashMap<String, Node>();
    int ccount = 0;
    for (List<String> cluster : clusters) {
      ccount++;
      if (ccount > maxcommunity) break;
      for (String nodeid : cluster)
        if (nodeid != null && !id2NodeMap.containsKey(nodeid)) // nodeid 有=null的时候???
        id2NodeMap.put(nodeid, new Node(NodeIdConveter.checkAndtoBytes(nodeid)));
    }
    try {
      addNodeNameForNodes(id2NodeMap.values());
    } catch (GdbException e1) {
      return generateErrorCodeJson("GdbException happens in query: " + e1.getMessage());
    }
    GetNodeClusterCBean bean = new GetNodeClusterCBean();
    bean.addEdges(graph);
    ccount = 0;
    for (List<String> nodeids : clusters) {
      ccount++;
      if (ccount > maxcommunity) break;
      List<ict.ada.gdb.rest.beans.model.Node> cluster = new ArrayList<ict.ada.gdb.rest.beans.model.Node>();
      for (String nodeid : nodeids) {
        if (nodeid == null) continue;
        Node node = id2NodeMap.get(nodeid);
        cluster.add(new ict.ada.gdb.rest.beans.model.Node(nodeid, node.getName(), node.getSnames(),
            NodeTypeMapper.getChannelName(node.getType().getChannel()), NodeTypeMapper
                .getAttributeName(node.getType().getAttribute()), null));
      }
      if (cluster.size() > 0) bean.addCluster(cluster);
    }
    // GetCliquesOfNodeBean bean = new GetCliquesOfNodeBean(nodes, mc.getCliques());

    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  /**
   * 缺少限制!
   * */
  public static String getGcommunity(String nodeId, String method) {
    String ret = null;
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    List<List<Node>> relNodeList = null;
    try {
      relNodeList = adaGdbService.getNodeCommunityPersonRelList(new Node(id), method);
      List<Node> totalNodes = new ArrayList<Node>();
      for (List<Node> list : relNodeList)
        totalNodes.addAll(list);
      addNodeNameForNodes(totalNodes);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    GetGcommunityBean bean = new GetGcommunityBean(relNodeList);
    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  private static Map<String, Map<String, Double>> genAdjGraphForNode(String nodeId, String type,
      int maxnode) throws GdbException {
    long total = 0L;
    RelationGraph relationGraph = null;
    Map<String, Map<String, Double>> adjGraph = new HashMap<String, Map<String, Double>>();
    byte[] id = NodeIdConveter.checkAndtoBytes(nodeId);
    // NodeType nodeType = NodeType.getType(Bytes.head(id, 2));
    Attribute attribute = NodeTypeMapper.getAttribute(type);
    Node seedNode = new Node(id);
    RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(seedNode);
    relbuilder.attribute(attribute);
    relbuilder.useRelRank(true);
    relbuilder.resultSize(maxnode);
    long start = System.currentTimeMillis();
    relationGraph = adaGdbService.queryRelationGraph(relbuilder.build());
    total = total + System.currentTimeMillis() - start;
    HashMap<ByteArray, String> nodeMap = new HashMap<ByteArray, String>();
    String centerNode = NodeIdConveter.toString(relationGraph.getCenterNode().getId());
    nodeMap.put(new ByteArray(relationGraph.getCenterNode().getId()), centerNode);
    HashMap<String, Double> list = new HashMap<String, Double>();
    List<RelQuerySpec> specList = new ArrayList<RelQuerySpec>();
    for (Edge edge : relationGraph.getCenterEdges()) {
      Node node = edge.getTail();
      String nodeIdStr = NodeIdConveter.toString(node.getId());
      nodeMap.put(new ByteArray(node.getId()), nodeIdStr);
      list.put(nodeIdStr, (double) edge.getEdgeWeight());
      RelQuerySpec.RelQuerySpecBuilder relbuilder1 = new RelQuerySpec.RelQuerySpecBuilder(node);
      relbuilder1.attribute(attribute);
      relbuilder1.useRelRank(true);
      // System.out.println(nodeType.getIntegerForm());
      RelQuerySpec spec = relbuilder1.build();
      specList.add(spec);
    }
    adjGraph.put(centerNode, list);
    start = System.currentTimeMillis();
    List<RelationGraph> relationGraphList = adaGdbService.queryRelationGraphsInParallel(specList,
        specList.size() / 16 + 1);
    System.out.println("relationGraphList.size()="+relationGraphList.size());
    total = total + System.currentTimeMillis() - start;
    for (RelationGraph relationGraph1 : relationGraphList) {
      list = new HashMap<String, Double>();
      for (Edge edge : relationGraph1.getCenterEdges()) {
        String nodeIdStr = nodeMap.get(new ByteArray(edge.getTail().getId()));
        if (nodeIdStr != null) {
          list.put(nodeIdStr, (double) edge.getEdgeWeight());
        }
      }
      adjGraph.put(nodeMap.get(new ByteArray(relationGraph1.getCenterNode().getId())), list);
    }
    for(Entry<String, Map<String, Double>> tails : adjGraph.entrySet()){
      for(Entry<String,Double> e : tails.getValue().entrySet()){
        if(!adjGraph.get(e.getKey()).containsKey(tails.getKey()))
          adjGraph.get(e.getKey()).put(tails.getKey(), e.getValue());
      }
    }
    return adjGraph;
  }

  /**
   * similarity
   * 
   * @param nodeId
   * @param start
   *          第一层节点要支持翻页
   * @param len
   *          第一层节点支持翻页
   * @param limit
   *          第二层机节点支持数量限制
   * @param weight
   *          复杂图要返回weight
   * @return
   */
  public static String getTwoLevelRelationGraph(String nodeId, int start, int len, int limit,
      boolean weight) {
    byte[] id = null;
    String ret = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    long s1 = System.currentTimeMillis();
    int page = start + len;
    // 1 获取第一层节点的直接关系节点
    Node seedNode = new Node(id);
    RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(seedNode);
    relbuilder.useRelRank(weight).attribute(Attribute.PERSON).resultSize(page);
    RelationGraph queryrelationGraph = null;
    try {
      queryrelationGraph = adaGdbService.queryRelationGraph(relbuilder.build());
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    RelationGraph relationGraph = new RelationGraph(queryrelationGraph.getCenterNode());
    if (queryrelationGraph.getOuterNodes().size() >= start) {
      Iterator<Edge> iter = queryrelationGraph.getCenterEdges().iterator();
      int count = 0;
      while (count < start) {
        iter.next();
        count++;
      }
      count = 0;
      while (count < len && iter.hasNext()) {
        relationGraph.addCenterEdge(iter.next());
        count++;
      }
    }
    // 2 批量获取第二层节点的直接关系节点
    List<RelQuerySpec> specList = new ArrayList<RelQuerySpec>();
    for (Node node : relationGraph.getOuterNodes()) {
      RelQuerySpec.RelQuerySpecBuilder relbuilder1 = new RelQuerySpec.RelQuerySpecBuilder(node);
      relbuilder1.useRelRank(weight).attribute(Attribute.PERSON).resultSize(limit);
      specList.add(relbuilder1.build());
    }
    int batchSize = specList.size() / (16 * 2) + 1;// 开多少个线程合适呢？现在暂定为32？？？？ 数据集中存在于edgeId表，开多线程意义大吗？
    List<RelationGraph> relationGraphList = null;
    try {
      relationGraphList = adaGdbService.queryRelationGraphsInParallel(specList, batchSize);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    ArrayList<Node> nodes = new ArrayList<Node>();
    nodes.add(relationGraph.getCenterNode());
    nodes.addAll(relationGraph.getOuterNodes());
    for (RelationGraph relationGraph1 : relationGraphList)
      nodes.addAll(relationGraph1.getOuterNodes());
    System.out.println(System.currentTimeMillis() - s1);
    s1 = System.currentTimeMillis();
    try {
      addNodeNameForNodes(nodes);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    System.out.println(System.currentTimeMillis() - s1);
    s1 = System.currentTimeMillis();
    GetTowLevelRelationGraphBean bean = new GetTowLevelRelationGraphBean();
    // bean.addNode(relationGraph.getCenterNode());
    Iterator<Edge> iter = relationGraph.getCenterEdges().iterator();
    for (Node node : relationGraph.getOuterNodes())
      bean.addEdge(iter.next(), node,
          NodeIdConveter.toString(relationGraph.getCenterNode().getId()));
    bean.addNode(relationGraph.getCenterNode());
    for (RelationGraph relationGraph1 : relationGraphList) {
      iter = relationGraph1.getCenterEdges().iterator();
      for (Node node : relationGraph1.getOuterNodes())
        if (!Bytes.equals(relationGraph.getCenterNode().getId(), node.getId())) // 去除回边
        bean.addEdge(iter.next(), node,
            NodeIdConveter.toString(relationGraph1.getCenterNode().getId()));
        else iter.next();
    }
    ret = PojoMapper.toJson(bean, true);
    System.out.println(System.currentTimeMillis() - s1);
    return ret;
  }

  private static Map<ByteArray, Collection<ByteArray>> getOuterNodeWithWdeIds(
      RelationGraph relationGraph, int weight) throws GdbException {
    Map<ByteArray, Collection<ByteArray>> outerNodeWithWdeIds = new HashMap<ByteArray, Collection<ByteArray>>();
    Iterator<Edge> edgeIter = relationGraph.getCenterEdges().iterator();
    long start = System.currentTimeMillis();
    ArrayList<Future<Pair<Edge, List<WdeRef>>>> results = new ArrayList<Future<Pair<Edge, List<WdeRef>>>>();
    while (edgeIter.hasNext()) {
      final Edge edge = edgeIter.next();

      final Relation relation = edge.getRelations().get(0);

      results.add(exec.submit(new Callable<Pair<Edge, List<WdeRef>>>() {
        public Pair<Edge, List<WdeRef>> call() throws GdbException {// 异常处理
          Pair<Edge, List<WdeRef>> re = new Pair<Edge, List<WdeRef>>();
          re.setFirst(edge);
          re.setSecond(adaGdbService.getRelationDetail(relation).getWdeRefs());
          return re;
        }
      }));
      // outerNodeWithWdeIds.put(nodeId, wdeids);
    }
    for (Future<Pair<Edge, List<WdeRef>>> fs : results) {
      Pair<Edge, List<WdeRef>> res = null;
      try {
        res = fs.get();
        if (res.getFirst().getEdgeWeight() < weight) continue;
      } catch (InterruptedException e) {

        e.printStackTrace();
      } catch (ExecutionException e) {

        e.printStackTrace();
      }
      Edge edge = res.getFirst();
      ByteArray nodeId = new ByteArray(edge.getTail().getId());
      HashSet<ByteArray> wdeids = new HashSet<ByteArray>();
      for (WdeRef wr : res.getSecond())
        wdeids.add(new ByteArray(wr.getWdeId()));
      outerNodeWithWdeIds.put(nodeId, wdeids);
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start);
    return outerNodeWithWdeIds;
  }

  public static String getMap(String nodeId) {
    String ret = null;
    byte[] id = null;
    GetMapBean bean = new GetMapBean();
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    // Node node =new Node(id);
    // 1 获取于该节点有直接关系location 节点
    RelationGraph relGraph = null;
    try {
      relGraph = getNodeRelByIdInter(new Node(id), Channel.ANY, Attribute.LOCATION, false);
      addNodeNameForRelationGraph(relGraph);
    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    List<String> nodeNames = new ArrayList<String>(relGraph.getOuterNodes().size());
    for (Node node : relGraph.getOuterNodes()) {
      if (node.getName() != null && node.getName().length() != 0) nodeNames.add(node.getName()
          .substring(4));
    }
    QueryBean queryBean = new QueryBean();
    queryBean.setAddress(nodeNames);
    queryBean.source = "google";
    List<GeoCodeBean> locations = GeoCodeBean.query(queryBean);
    Iterator<GeoCodeBean> iter = locations.iterator();
    for (Node node : relGraph.getOuterNodes()) {
      // 2 获取位置的经纬度信息,默认都为00
      if (node.getName() == null || node.getName().length() == 0) continue;
      GeoCodeBean location = iter.next();

      String longitude = "00";
      String latitude = "00";
      if (location.getResult() != null
          && !location.getResult().get("status").getTextValue().equals("ZERO_RESULTS")) {
        longitude = String.valueOf(location.getResult().get("results").get(0).get("geometry")
            .get("location").get("lng").getValueAsDouble());
        latitude = String.valueOf(location.getResult().get("results").get(0).get("geometry")
            .get("location").get("lat").getValueAsDouble());
      }
      bean.addPlace(node, longitude, latitude, nodeId);

    }
    for (Edge edge : relGraph.getCenterEdges()) {
      bean.addEdge(edge);
    }
    return PojoMapper.toJson(bean, true);
  }

  public static void addNodeNameForNodes(Collection<Node> nodes1) throws GdbException {
    List<Node> nodes = new ArrayList<Node>(nodes1.size());
    for(Node node: nodes1){
      if(node.getName()==null)
        nodes.add(node);
    }
    Map<Channel, Iterator<Pair<String, List<String>>>> resultmap = new HashMap<Channel, Iterator<Pair<String, List<String>>>>();
    Map<Channel, List<byte[]>> idsmap = new HashMap<Channel, List<byte[]>>();// 取name时要保证同一个 通道
    for (Node node : nodes) {
      if (!idsmap.containsKey(node.getType().getChannel())) {
        List<byte[]> ids = new ArrayList<byte[]>();
        ids.add(node.getId());
        idsmap.put(node.getType().getChannel(), ids);
      } else idsmap.get(node.getType().getChannel()).add(node.getId());
    }
    for (Entry<Channel, List<byte[]>> e : idsmap.entrySet())
      resultmap.put(e.getKey(),
          adaGdbService.getNodeNameAndSnameByIdBatched(e.getValue(), e.getValue().size() / 16 + 1)
              .iterator());
    for (Node node : nodes) {
      Iterator<Pair<String, List<String>>> iter = resultmap.get(node.getType().getChannel());
      Pair<String, List<String>> pair = iter.next();
      if (pair.getFirst() == null) {
        // System.out.println();
        continue;
      }
      node.setName(pair.getFirst());
      for (String sname : pair.getSecond())
        node.addSearchName(sname);
    }
    // 对event再做一遍处理,从ada_event中取sname.这个步骤中尽量把边界情况考虑充分,尽量不要报错.
    List<Node> eventNodes = new ArrayList<Node>();
    for (Node node : nodes)
      if (node.getType().getAttribute().equals(Attribute.EVENT)) eventNodes.add(node);
    List<Integer> eventIds = new ArrayList<Integer>();
    // 用于标记有效的eventid,使得eventNodes和List<AdaEventBean> events保持同步!
    boolean[] flag = new boolean[eventNodes.size()];
    int index = 0;
    for (Node event : eventNodes) {
      String eventName = event.getName();
      String eventId = eventName.substring(4);
      // 对一些特殊情况处理,比如虽然nodeType是event,但是其id并不是ada_event中的id,
      if (eventId.matches("^[0-9]+$")) {
        eventIds.add(Integer.parseInt(eventId));
        flag[index] = true;
      } else flag[index] = false;
      index++;
    }
    if (eventIds == null || eventIds.size() == 0) return;
    List<AdaEventBean> events = null;
    try {
      events = adaEventDao.getNodesByIds(eventIds);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    Iterator<Node> eventNodesIter = eventNodes.iterator();
    Iterator<AdaEventBean> eventsIter = events.iterator();
    for (boolean iflag : flag) {
      if (iflag) {
        AdaEventBean bean = eventsIter.next();
        Node eventNode = eventNodesIter.next();
        if (bean == null) continue;
        List<String> sname = null;
        if (bean.getT() == null || bean.getT().equals("")) {
          int toindex = 5;
          if (bean.getTags().size() < 5) toindex = bean.getTags().size();
          sname = bean.getTags().subList(0, toindex);
        } else {
          sname = new ArrayList<String>();
          sname.add(bean.getT());
        }
        eventNode.setSnames(sname);

      } else eventNodesIter.next();
    }
    for(Node node : nodes1){
      checkNode(node);
    }
  }

  private static void addNodeNameForRelationGraph(RelationGraph graph) throws GdbException {
    addNodeNameForNodes(graph.getOuterNodes());
  }
  /**
   * check node, if node's sname == null, then add name to sname
   * */
  private static void checkNode(Node node){
    if(node != null){
      if(node.getSnames() == null || node.getSnames().isEmpty()){
        if(node.getName() != null){
          node.addSearchName(node.getName());
        }
      }
    }
  }
  public static String getRecommendOfNode(String nodeId) {
    byte[] id = null;
    String ret = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    Node node = new Node(id);
    String propertyKey = "rec_" + String.valueOf(node.getType().getIntegerForm());
    if (channelService.containsKey(propertyKey)) {
      String url = channelService.getProperty(propertyKey);
      url = url + nodeId + "/recommend";
      ret = InternalServiceResources.downloadHtml(url);
    } else {
      ret = "{\"results\":[],\"errorCode\":\"success\"}";
    }
    return ret;
  }

  public static String getEventTrackInfo(String nodeId) {
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }
    Node node = new Node(id);
    String[] relationTypes = { "d-reason", "d-result", "main-event", "reason", "result",
        "sub-event" };
    // int[] subEventRelA={131002,131003,131004,131005,131006,131007,131008};
    HashSet<String> mainEventRel = new HashSet<String>();
    HashSet<String> subEventRel = new HashSet<String>();
    mainEventRel.add("d-reason");
    mainEventRel.add("d-result");
    mainEventRel.add("main-event");
    subEventRel.add("reason");
    subEventRel.add("result");
    subEventRel.add("sub-event");
    RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(node);
    relbuilder.attribute(Attribute.EVENT);
    for (String relType : relationTypes)
      relbuilder.relType(RelationType.getType(relType));
    RelationGraph mainEvent = null;
    try {
      RelationGraph graph = adaGdbService.queryRelationGraph(relbuilder.build());// TODO 结果可能为空，处理？
      if (graph.getCenterEdges() != null && graph.getCenterEdges().size() != 0) {
        Relation rel = graph.getCenterEdges().iterator().next().getRelations().get(0);
        if (mainEventRel.contains(rel.getType().getStringForm())) mainEvent = graph;
        else {
          RelQuerySpec.RelQuerySpecBuilder relbuilder1 = new RelQuerySpec.RelQuerySpecBuilder(rel
              .getParentEdge().getTail());
          relbuilder1.attribute(Attribute.EVENT);
          for (String relType : mainEventRel)
            relbuilder1.relType(RelationType.getType(relType));
          mainEvent = adaGdbService.queryRelationGraph(relbuilder1.build());
        }
      }

    } catch (GdbException e) {
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    }
    GetEventTrackInfoBean bean = new GetEventTrackInfoBean();
    // TODO mainEvent为空
    try {
      if (mainEvent != null) {
        addNodeNameForRelationGraph(mainEvent);
        Node nodeName = adaGdbService.getNodeNameAndSnameById(mainEvent.getCenterNode().getId());
        String eventId = nodeName.getName().substring(4);
        List<Integer> eventIds = new ArrayList<Integer>();
        eventIds.add(Integer.parseInt(eventId));
        for (Node event : mainEvent.getOuterNodes()) {
          eventId = event.getName().substring(4);
          eventIds.add(Integer.parseInt(eventId));
        }
        List<AdaEventBean> eventName = adaEventDao.getNodesByIds(eventIds);
        String title = eventName.get(0).getT();
        int id1 = 1;
        List<Pair<Node, List<WdeRef>>> timeNodes = getTimeRelation(mainEvent.getCenterNode());
        for (Pair<Node, List<WdeRef>> timeNode : timeNodes) {
          Node time = timeNode.getFirst();
          DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
          Date date = format.parse(time.getName().substring(4));
          DateFormat format1 = new SimpleDateFormat("yyyy/MM/dd");
          String dateS = format1.format(date);
          for (WdeRef wdeRef : timeNode.getSecond())
            bean.addEvent(id1, 1, title, "主事件", dateS, eventName.get(0).getAb(), wdeRef);
        }
        int index = 1;
        for (Edge edge : mainEvent.getCenterEdges()) {
          String ab = eventName.get(index++).getAb();
          List<Pair<Node, List<WdeRef>>> timeNodes1 = getTimeRelation(edge.getTail());
          String group = null;
          int type = -1;
          if (edge.getRelations().get(0).getType() == RelationType.getType("d-reason")) {
            group = "原因";
            type = 3;
          } else if (edge.getRelations().get(0).getType() == RelationType.getType("d-result")) {
            group = "结果";
            type = 4;
          } else if (edge.getRelations().get(0).getType() == RelationType.getType("d-mainevent")) {
            group = "过程";
            type = 2;

          } else {
            group = "?";
          }
          for (Pair<Node, List<WdeRef>> timeNode : timeNodes1) {
            Node time = timeNode.getFirst();
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");
            Date date = format.parse(time.getName().substring(4));
            DateFormat format1 = new SimpleDateFormat("yyyy/MM/dd");
            String dateS = format1.format(date);
            for (WdeRef wdeRef : timeNode.getSecond())
              bean.addEvent(id1, type, title, group, dateS, ab, wdeRef);
          }
        }

      }
    } catch (GdbException e) {
      e.printStackTrace();
      return generateErrorCodeJson("GdbException happens in query: " + e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      return generateErrorCodeJson(e.getMessage());
    }
    String ret = "";

    ret = PojoMapper.toJson(bean, true);
    return ret;
  }

  public static String getNodesRelRelativeNodes(String nodeids, String type, int start, int len) {
    String ret = null;
    List<byte[]> nodeIds = new ArrayList<byte[]>();

    try {
      String[] nodeidArray = nodeids.split(",");
      for (String nodeid : nodeidArray)
        nodeIds.add(NodeIdConveter.checkAndtoBytes(nodeid));
    } catch (Exception e) {
      return generateErrorCodeJson(e.getMessage());
    }

    Attribute attribute = null;
    if (type != null) {
      // 取实体，对不是实体进行过滤（category 和identity）
      if (type.equals("all")) attribute = Attribute.ANY;
      else if (type.equals("entity")) attribute = Attribute.ANY;
      else attribute = NodeTypeMapper.getAttribute(type);
    } else attribute = Attribute.ANY;

    List<RelQuerySpec> specList = new ArrayList<RelQuerySpec>(nodeIds.size());
    for (byte[] nodeId : nodeIds) {
      RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(new Node(
          nodeId));
      if (attribute != null) {
        relbuilder.attribute(attribute);
        // System.out.println(nodeType.getIntegerForm());
      }
      relbuilder.resultSize(10000);
      // if (relTypeList != null) for (RelationType relType : relTypeList)
      // relbuilder.relType(relType);
      specList.add(relbuilder.build());
    }

    GetNodesRelRelativeNodesBean bean = null;
    try {
      List<RelationGraph> graphs = adaGdbService.queryRelationGraphsInParallel(specList,
          specList.size() / 16 + 1);
      HashSet<ByteArray> unionNodes = new HashSet<ByteArray>();
      Iterator<RelationGraph> graphIter = graphs.iterator();
      for (Node node : graphIter.next().getOuterNodes())
        unionNodes.add(new ByteArray(node.getId()));
      HashSet<ByteArray> unionNodesTmp = null;
      while (graphIter.hasNext()) {
        unionNodesTmp = new HashSet<ByteArray>();
        for (Node node : graphIter.next().getOuterNodes()) {
          ByteArray nodeId = new ByteArray(node.getId());
          if (unionNodes.contains(nodeId)) unionNodesTmp.add(nodeId);
        }
        unionNodes = unionNodesTmp;
        if (unionNodes.size() == 0) break;
      }

      List<Node> unionNodeList = new ArrayList<Node>(unionNodes.size());
      for (ByteArray nodeId : unionNodes)
        unionNodeList.add(new Node(nodeId.getBytes()));
      int count = unionNodeList.size();
      start = start < 0 ? 0 : start;
      int end = start + len;
      end = end > unionNodeList.size() ? unionNodeList.size() : end;
      if (start >= end) unionNodeList = Collections.emptyList();
      else unionNodeList = unionNodeList.subList(start, end);

      addNodeNameForNodes(unionNodeList);
      bean = new GetNodesRelRelativeNodesBean(unionNodeList, count);
    } catch (GdbException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return PojoMapper.toJson(bean, true);
  }
  /* private method* */

  private static List<Pair<Node, List<WdeRef>>> getTimeRelation(Node node) throws GdbException {
    RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(node);
    relbuilder.attribute(Attribute.TIME).relType(RelationType.getType("event-relation"));
    RelationGraph graph = adaGdbService.queryRelationGraph(relbuilder.build());
    List<Pair<Node, List<WdeRef>>> result = new ArrayList<Pair<Node, List<WdeRef>>>();
    for (Edge timeEdge : graph.getCenterEdges()) {
      Pair<Node, List<WdeRef>> pair = new Pair<Node, List<WdeRef>>();
      // timeEdge.getRelations();
      List<WdeRef> wdeRefs = adaGdbService.getRelationDetail(timeEdge.getRelations().get(0))
          .getWdeRefs();
      pair.setFirst(timeEdge.getTail());
      pair.setSecond(wdeRefs);
      result.add(pair);
    }
    List<Node> timeNodes = new ArrayList<Node>();
    for (Pair<Node, List<WdeRef>> timeNode : result)
      timeNodes.add(timeNode.getFirst());
    addNodeNameForNodes(timeNodes);
    return result;

  }

  public static String generateErrorCodeJson(String errorCode) {
    Map<String, String> errorCodeMap = new HashMap<String, String>();
    if (errorCode == null || errorCode.isEmpty()) errorCode = "errorCode";
    errorCodeMap.put("errorCode", errorCode);
    String errorCodeJson = PojoMapper.toJson(errorCodeMap, true); // assert no
    // exception
    return errorCodeJson;
  }

  public static RelationGraph getNodeRelByIdInter(Node node, Channel channel, Attribute type,
      boolean getweight) throws GdbException {
    RelationGraph relationGraph = null;
    Attribute nodeType = null;
    // if (type != null) {
    // 取实体，对不是实体进行过滤（category 和identity）
    // if (type.equals("entity")) nodeType = NodeType.ANY_NODE;
    // nodeType = NodeType.getType(StrTypeMapper.ConvertToIntType(type));
    // } else nodeType = NodeType.ANY_NODE;
    // TimeRange tr=new TimeRange(0,System.currentTimeMillis()/1000);
    RelQuerySpec.RelQuerySpecBuilder specBuilder = new RelQuerySpec.RelQuerySpecBuilder(node)
        .attribute(type);
    specBuilder.resultSize(Integer.MAX_VALUE);
    // specBuilder.timeRange(tr);
    if (getweight) specBuilder.useRelRank(true);// 采用排序接口,resultsize 默认为10000需不需要调整？
    relationGraph = adaGdbService.queryRelationGraph(specBuilder.build());
    if (type != null && type.equals("entity")) { // 对非实体进行删除（CATEGORY 和
                                                 // IDENTITY）[新版里还有entity这种类型么Attribute.ENTITY]
      ArrayList<Edge> edgeToDel = new ArrayList<Edge>();
      for (Edge edge : relationGraph.getCenterEdges()) {
        if (edge.getTail().getType().getAttribute().equals(Attribute.CATEGORY)) edgeToDel.add(edge);
      }
      for (Edge edge2 : edgeToDel)
        relationGraph.removeCenterEdgeAndRelatedOuterNode(edge2);
    }

    return relationGraph;
  }

  private static RelationGraph getNodeRelByIdInter(Node node, Channel channel, Attribute type,
      boolean getweight, int limit, List<RelationType> relTypeList) throws GdbException {
    RelationGraph relationGraph = null;
    RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(node);
    relbuilder.attribute(type);
    if (relTypeList != null) for (RelationType relType : relTypeList)
      relbuilder.relType(relType);
    relbuilder.useRelRank(getweight);
    relbuilder.resultSize(limit);
    RelQuerySpec spec = relbuilder.build();
    relationGraph = adaGdbService.queryRelationGraph(spec);
    return relationGraph;
  }

  public static RelationGraph getNodeRelByIdInter(Node node, Channel channel, Attribute type,
      boolean getweight, TimeRange tr, boolean disambiguation, List<byte[]> wdeIds,
      Collection<RelationType> relTypes) throws GdbException {
    RelationGraph relationGraph = null;
    NodeType nodeType = null;
    // if (type != null) {
    // 取实体，对不是实体进行过滤（category 和identity）
    // if (type.equals("entity")) nodeType = NodeType.ANY_NODE;
    // else nodeType = NodeType.getType(StrTypeMapper.ConvertToIntType(type));
    // } else nodeType = NodeType.ANY_NODE;
    // TimeRange tr=new TimeRange(0,System.currentTimeMillis()/1000);
    RelQuerySpec.RelQuerySpecBuilder specBuilder = new RelQuerySpec.RelQuerySpecBuilder(node)
        .attribute(type).timeRange(tr);
    for (RelationType relType : relTypes)
      specBuilder.relType(relType);
    specBuilder.resultSize(Integer.MAX_VALUE);
    // specBuilder.timeRange(tr);
    if (getweight) specBuilder.useRelRank(true);// 采用排序接口,resultsize 默认为10000需不需要调整？
    if (disambiguation) for (byte[] wdeId : wdeIds)
      specBuilder.wdeId(wdeId);
    relationGraph = adaGdbService.queryRelationGraph(specBuilder.build());
    if (type != null && type.equals("entity")) { // 对非实体进行删除（CATEGORY 和
                                                 // IDENTITY）[新版里还有entity这种类型么Attribute.ENTITY]
      ArrayList<Edge> edgeToDel = new ArrayList<Edge>();
      for (Edge edge : relationGraph.getCenterEdges()) {
        if (edge.getTail().getType().getAttribute().equals(Attribute.CATEGORY)) edgeToDel.add(edge);
      }
      for (Edge edge2 : edgeToDel)
        relationGraph.removeCenterEdgeAndRelatedOuterNode(edge2);
    }

    return relationGraph;
  }

  public static void main(String[] args) throws FileNotFoundException, GdbException {

    // System.out.println(getNodeNameFromInternalIndexSingle("北京", "weibo", 0, 0));

    /*
     * System.out.println(InternalNodeService.getAttributeSourceById(
     * "c801c42a30607f3dc25f03af7d27f178b0b1", "telephone", "6306868")); Node node = null; try {
     * node = adaGdbService.getNodeAttributes(new Node(NodeIdConveter
     * .checkAndtoBytes("c801c42a30607f3dc25f03af7d27f178b0b1"))); } catch (GdbException e) { //
     * TODO Auto-generated catch block e.printStackTrace(); } if (node.getAttributes() != null) {
     * for (NodeAttribute nodeAttribute : node.getAttributes()) { String key =
     * nodeAttribute.getKey(); // GetAttributesByIdBean.NodeAttribute attr = bean.new
     * NodeAttribute(); for (AttrValueInfo valueInfo : nodeAttribute.getValues()) {
     * System.out.println(key + "  :   " + valueInfo.getValue()); } } } long start1 =
     * System.currentTimeMillis(); System.out.println(InternalNodeService.getNodeSuperiorById(
     * "e602a28f1e96b30e46f2bb1c1625ee4f7f73", "1234455")); long end1 = System.currentTimeMillis();
     * System.out.println(end1 - start1);
     */
    // System.out.println(InternalNodeService.getAttributesById("c801c42a30607f3dc25f03af7d27f178b0b1"));
    // System.out.println(InternalNodeService.getRecommendOfNode("d20117dd33ffd20cb3462064c82c6f7e6422"));
    String nodeId = "01358950a2469278bacc690fa71f82446c4e";
    // //System.out.println(InternalNodeService.getNodeClusterC("c801c42a30607f3dc25f03af7d27f178b0b1","any_person"));
    // //
    // System.out.println(InternalNodeService.getAttributesById("01358950a2469278bacc690fa71f82446c4e",
    // true, "0035000005f739a4"));
    // System.out.println(InternalNodeService.getAttributeSourceById("01358950a2469278bacc690fa71f82446c4e",
    // "industry", "Research"));
    // System.out.println(InternalNodeService.getPathBetNodes2("153517a5d4bb6446d013f7c27d49d4dc40d9",
    // "15351b44d3178cc66b5be509960324399a12", 4, 1000));
    System.out.println(InternalNodeService.getTwoLevelRelationGraph(nodeId, 0, 100, 11, true));

    System.out.println(InternalNodeService.getCliquesOfNode(nodeId, "org", 1000, 1000));

    // System.out.println(InternalRelationService.getRelationSourceById(relationId));

    // System.out.println(InternalNodeService.getNodeHierarchyById(nodeId));
    // System.out.println(InternalNodeService.getNodeSuperiorById(nodeId, "addition"));
    // System.out.println(InternalNodeService.getNodeRelNodeClusterById(nodeId,"all", 3, 20, 10000,
    // 1));
    // System.out.println(InternalNodeService.getNodeRelNodeClusterById(nodeId, type, mincluster,
    // maxcluster, maxnode, weight));

    // System.out.println(getMap("c801c42a30607f3dc25f03af7d27f178b0b1"));
    PrintStream ps = new PrintStream(new File("/home/wangqianbo/test/test1"));
    // ps.println(
    // InternalNodeService.getTwoLevelRelationGraph("c80474c10b9e11d8754dc7ca823033e32650",0,10,10,true));
    long start1 = System.currentTimeMillis();
    // ps.println(InternalNodeService.getNodeRelById("c801c42a30607f3dc25f03af7d27f178b0b1",
    // "web"));
    // ps.println(InternalNodeService.getNodeRelById("c801b7fc93c67fce899ea506e450bb41c321", null,
    // false, 0, 20, -1, -1, "None", false, null));
    // ps.println(InternalNodeService.getCliquesOfNode("c801c42a30607f3dc25f03af7d27f178b0b1"));
    // InternalNodeService.getCliquesOfNode("c801c42a30607f3dc25f03af7d27f178b0b1",1000);
    // InternalNodeService.getCliquesOfNode("c801b7c8c21c85b2200ac4984c2d815b46e7");
    long end1 = System.currentTimeMillis();
    System.out.println(end1 - start1);
    // System.out.println(InternalNodeService.getNodeNameById("c801c414f2010baf798444bfa7ddb260a079"));
    // System.out.println(InternalNodeService.getNodeIdByName(null, "web"));
    // InternalNodeService.getNodeRelById("c801c42a30607f3dc25f03af7d27f178b0b1","web" );
    // ps.println(InternalNodeService.getNodeRelNodeClusterById("c801c42a30607f3dc25f03af7d27f178b0b1",
    // "web"));
    long start = System.currentTimeMillis();
    // ps.println(InternalNodeService.getNodeRelNodeClusterById("c801c42a30607f3dc25f03af7d27f178b0b1",
    // "web",5));
    // ps.println(InternalNodeService.getRelationTimeLineByNodeId("c801c42a30607f3dc25f03af7d27f178b0b1"));
    // System.out.println(InternalNodeService.getAttributeSourceById("c801c42a30607f3dc25f03af7d27f178b0b1",
    // "birth_place", "中国"));
    // ps.println(InternalNodeService.getPathBetNodes("c802d4c53e045701d67e127a3b7aa6988c39","c802c97803e6b83d771dfe3f753e2a36dcc9",
    // 6, 64));
    // System.out.println( InternalNodeService.getGcommunity("c8047d37940a0260eac65b7a04df9f8abeaf",
    // "cm_hm"));
    // ps.println(InternalNodeService.getPathBetNodes2("e602592739029ee47d8c97b1a29e050debb8",
    // "e602a72a673fa2ea1d27f687adcd2e4a5258", 4, 1000));
    // System.out.println(InternalNodeService.getNodeRelById("dc016091c2a47adc449279f9fb5f124ca809",null,
    // true, 0, 3, -1, -1, "None", false, null));
    // ps.println(InternalNodeService.getTableRowCount("scholar"));
    long end = System.currentTimeMillis();
    // System.out.println(end - start);
  }
}
