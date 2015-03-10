package ict.ada.gdb.rest.test;

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
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.PathQuerySpec;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.common.TimeRange;
import ict.ada.gdb.rest.beans.GetAttributesByIdBean;
import ict.ada.gdb.rest.beans.GetNodeIdByNameBean;
import ict.ada.gdb.rest.beans.GetPathBetNodes2Bean;
import ict.ada.gdb.rest.beans.GetRelationByNodeIdBean;
import ict.ada.gdb.rest.beans.GetTowLevelRelationGraphBean;
import ict.ada.gdb.rest.beans.NodeStatistic;
import ict.ada.gdb.rest.common.DfsPathGraph;
import ict.ada.gdb.rest.common.Path;
import ict.ada.gdb.rest.services.InternalServiceResources;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.rest.util.NodeIdConveter;
import ict.ada.gdb.service.AdaGdbService;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;

public class TestActions {
  static AdaGdbService adaGdbService = InternalServiceResources.getAdaGdbService();
  private static final  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static long seed = 0;
  private static long start = 0;
  static{
    try {
      start = sdf.parse("2012-01-01 00:00:00").getTime();
      seed = sdf.parse("2014-01-01 00:00:00").getTime() - start;
    } catch (ParseException e) {
      e.printStackTrace();
    }
   
  }
  public static StatisticsInfoBean testGetAttr(String name) {
    StatisticsInfoBean result = new StatisticsInfoBean();
    result.setType(1);
    result.addArg("name",name );
    long start = System.currentTimeMillis();
    result.setTs(start);
    GetNodeIdByNameBean bean = getNodeIdByName(name);
    if(bean == null) {
      result.setDur(System.currentTimeMillis()  - start);
    }else{
      GetAttributesByIdBean attrs = getAttributesById(bean.getNode().getId());
      result.setDur(System.currentTimeMillis()  - start);
      if(attrs == null)
        result.addStatistic("attrCount", "-1");
      else
        result.addStatistic("attrCount", String.valueOf(attrs.getAttrsList().size()));
    }
    return result;
  }

  public static StatisticsInfoBean testGetPathBetTwoNodes(String startNode, String endNode){
    StatisticsInfoBean result = new StatisticsInfoBean();
    result.setType(2);
    long start = System.currentTimeMillis();
    result.setTs(start);
    GetNodeIdByNameBean node1 = getNodeIdByName(startNode);
    GetNodeIdByNameBean node2 = getNodeIdByName(endNode);
    result.addArg("startNode", startNode);
    result.addArg("endNode", endNode);
    result.addArg("degree", "2");
    if(node1 == null || node2 == null){
      result.setDur(System.currentTimeMillis() - start);
    }else{
      GetPathBetNodes2Bean paths = getPathBetNodes2(node1.getNode().getId(),node2.getNode().getId(),3,10000,null);
      result.setDur(System.currentTimeMillis()-start);
      if(paths == null){
        result.addStatistic("pathCount", "-1");
        result.addStatistic("nodeCount", "-1");
      }else{
        result.addStatistic("pathCount", String.valueOf(paths.getCount()));
        result.addStatistic("nodeCount", String.valueOf(paths.getNodeList().size()));
      }
     
    }
    return result;
  }
  
  public static StatisticsInfoBean testGetNodeRelById(String nodeName){
    StatisticsInfoBean result = new StatisticsInfoBean();
    result.setType(3);
    int st = (int)((long) (start +Math.random()*seed)/1000);
    int et =  (int)((long) (start +Math.random()*seed)/1000);
    if(st > et){
      int tmp = st;
      st = et;
      et = tmp;
    }
    long start = System.currentTimeMillis();
    result.addArg("node", nodeName);
//    result.addArg("relType", relType);
    result.addArg("st", String.valueOf(st));
    result.addArg("et", String.valueOf(et));
    GetNodeIdByNameBean node = getNodeIdByName(nodeName);
    if(node == null){
      result.setDur(System.currentTimeMillis() - start);
    }else{
      GetRelationByNodeIdBean relations = getNodeRelById(node.getNode().getId(), "weibo", "account", true, 0, 10000,st,et, false, "all", false);
      result.setDur(System.currentTimeMillis() - start);
      if(relations  == null){
        result.addStatistic("relCount","-1");
      }else{
        result.addStatistic("relCount", String.valueOf(relations.getEdgeList().size()));
      }
    }
    return result;
  }
  
  public static StatisticsInfoBean testGetTwoLevelRelationGraph(String nodeName){
    StatisticsInfoBean result = new StatisticsInfoBean();
    result.setType(5);
    long start = System.currentTimeMillis();
    result.addArg("node", nodeName);
    GetNodeIdByNameBean node = getNodeIdByName(nodeName);
    if(node == null){
      result.setDur(System.currentTimeMillis() - start);
    }else{
      GetTowLevelRelationGraphBean rels = getTwoLevelRelationGraph(node.getNode().getId(),true);
      result.setDur(System.currentTimeMillis() - start);
      if(rels == null){
        result.addStatistic("edgeCount", "-1");
        result.addStatistic("nodeCount", "-1");
      }else{
        result.addStatistic("edgeCount", String.valueOf(rels.getEdgeList().size()));
        result.addStatistic("nodeCount", String.valueOf(rels.getNodeList().size()));
      }
    }
    return result;
  }
  // 传过来的name是否会有前缀呢?
  public static GetNodeIdByNameBean getNodeIdByName(String name) {
    byte[] id = null;
    String ret = null;
    String stringId = null;
    Channel channel = null;
    Attribute attribute = null;
    if (name == null) {
      return null;
    }
    NodeType nodeType = NodeType.getType(Channel.WEIBO, Attribute.ACCOUNT);
    try {
      id = adaGdbService.getNodeIdByName(nodeType.getStringForm()+name, nodeType);
    } catch (GdbException e) {
      return null;
    }
    if (id != null) {
      stringId = NodeIdConveter.toString(id);
    } else
      return null;
    GetNodeIdByNameBean bean = new GetNodeIdByNameBean();
    ict.ada.gdb.rest.beans.model.Node node =
        new ict.ada.gdb.rest.beans.model.Node(stringId, name, null,
            NodeTypeMapper.getChannelName(channel), NodeTypeMapper.getAttributeName(attribute),
            null);
    bean.setNode(node);
    return bean;
  }

  static GetAttributesByIdBean getAttributesById(String nodeId) {
    Node nodeAttr = null;
    String ret = null;
    byte[] id = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return null;
    }
    Node node = new Node(id);
    try {
      nodeAttr = adaGdbService.getNodeAttributes(node, true);
    } catch (GdbException e) {
      return null;
    }
    GetAttributesByIdBean bean = getAttributesInter(nodeAttr);
    return bean;
  }

  public static GetAttributesByIdBean getAttributesInter(Node nodeAttr) {
    GetAttributesByIdBean bean = new GetAttributesByIdBean();
    bean.initAttrsList();
    if (nodeAttr.getAttributes() != null) {
      for (NodeAttribute nodeAttribute : nodeAttr.getAttributes()) {
        String key = nodeAttribute.getKey();
        for (AttrValueInfo valueInfo : nodeAttribute.getValues()) {
          ict.ada.gdb.rest.beans.model.NodeAttribute attr =
              new ict.ada.gdb.rest.beans.model.NodeAttribute();
          attr.setKey(key);
          attr.setValue(valueInfo.getValue());
          attr.setCount(valueInfo.getWdeRefCount());

          List<ict.ada.gdb.rest.beans.model.WdeRef> wdeRefs = null;
          if (valueInfo.getWdeRefs() != null && valueInfo.getWdeRefs().size() != 0) {
            wdeRefs =
                new ArrayList<ict.ada.gdb.rest.beans.model.WdeRef>(valueInfo.getWdeRefs().size());
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

  public static GetPathBetNodes2Bean getPathBetNodes2(String start, String end, int degree,
      int max, String type) {
    byte[] startId = null;
    byte[] endId = null;
    PathGraph pathgraph = null;
    String ret = null;
    try {
      startId = NodeIdConveter.checkAndtoBytes(start);
      endId = NodeIdConveter.checkAndtoBytes(end);
      if (Bytes.equals(startId, endId))
        throw new IllegalArgumentException("path start and end can't be the same");
    } catch (Exception e) {
      return null;
    }
    NodeType startType = NodeType.getType(startId[0], startId[1]);
    NodeType endType = NodeType.getType(endId[0], endId[1]);
    Attribute attribute = null;
    if (type != null) {
      if (type.equals("all"))
        attribute = Attribute.ANY;
      else if (type.equals("NOEXISTS")) {
        if (!startType.equals(endType))
          return null;
        attribute = startType.getAttribute();
      } else {
        attribute = NodeTypeMapper.getAttribute(type);
        if (!startType.equals(endType) || startType.getAttribute() != attribute)
          return null;
      }
    }

    if (attribute == null)
      attribute = startType.getAttribute();
    HashSet<Node> nodes = new HashSet<Node>();
    HashSet<ByteArray> edges = new HashSet<ByteArray>();

    long starttime = Timer.now();
    PathQuerySpec.PathQuerySpecBuilder builder =
        new PathQuerySpec.PathQuerySpecBuilder(startId, endId);
    builder.maxPathLength(degree - 1);
    builder.requiredAttribute(attribute);
    try {
      pathgraph = adaGdbService.queryPathGraph(builder.build());
    } catch (GdbException e) {
      return null;
    }
    // ps.println("InternalNodeService: Get pathgraph in " +
    // Timer.msSince(starttime) + "ms " );
    starttime = Timer.now();
    DfsPathGraph d = new DfsPathGraph(max, degree, pathgraph);
    d.maindfs();
    // ps.println("InternalNodeService: gen paths in " +
    // Timer.msSince(starttime) + "ms " );
    starttime = Timer.now();
    for (Path path : d.getMinheap().getHeap()) {
      if (path == null)
        continue;
      int len = path.getNodeList().size();
      for (int i = 0; i < len - 1; i++) {
        nodes.add(path.getNodeList().get(i));
      }
    }
    // ps.println("InternalNodeService: get prepare  in " +
    // Timer.msSince(starttime) + "ms " );
    long starttime1 = Timer.now();
    nodes.add(pathgraph.getGraphStart());
    nodes.add(pathgraph.getGraphEnd());

    try {
      addNodeNameForNodes(nodes);
    } catch (GdbException e1) {
      return null;
    }

    // ps.println("InternalNodeService: get names  in " +
    // Timer.msSince(starttime1) + "ms " );
    starttime1 = Timer.now();
    GetPathBetNodes2Bean bean = new GetPathBetNodes2Bean();
    bean.addPaths(d.getMinheap().getHeap());
    bean.addNodes(nodes);
    // bean.addEdges(edges1);
    // ps.println("InternalNodeService: gen  bean  in " +
    // Timer.msSince(starttime1) + "ms " );
    return bean;
  }

  public static void addNodeNameForNodes(Collection<Node> nodes1) throws GdbException {
    List<Node> nodes = new ArrayList<Node>(nodes1.size());
    for (Node node : nodes1) {
      if (node.getName() == null)
        nodes.add(node);
    }
    Map<Channel, Iterator<Pair<String, List<String>>>> resultmap =
        new HashMap<Channel, Iterator<Pair<String, List<String>>>>();
    Map<Channel, List<byte[]>> idsmap = new HashMap<Channel, List<byte[]>>();// 取name时要保证同一个
    // 通道
    for (Node node : nodes) {
      if (!idsmap.containsKey(node.getType().getChannel())) {
        List<byte[]> ids = new ArrayList<byte[]>();
        ids.add(node.getId());
        idsmap.put(node.getType().getChannel(), ids);
      } else
        idsmap.get(node.getType().getChannel()).add(node.getId());
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
  }

  static GetRelationByNodeIdBean getNodeRelById(String nodeId, String channelName, String type,
      boolean relInfo, int start, int len, long st, long et, boolean disambiguation,
      String relations, boolean staticsticB) {
    String ret = null;
    byte[] id = null;
    Attribute attribute = null;
    Channel channel = null;
    boolean getweight = relInfo || staticsticB;
    try {
      if (channelName == null)
        channel = Channel.ANY;
      else
        channel = NodeTypeMapper.getChannel(channelName);
      if (type == null)
        attribute = Attribute.ANY;
      else
        attribute = NodeTypeMapper.getAttribute(type);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
    RelationGraph relationGraph = null;
    RelationGraph queryrelationGraph = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      e.printStackTrace();
      return null;

    }
    if (st == -1)
      st = Long.MIN_VALUE;
    if (et == -1)
      et = Long.MAX_VALUE;
    HashSet<RelationType> relationTypes = new HashSet<RelationType>();
    if (!relations.equals("all"))
      for (String relation : relations.split(",")) {
        relationTypes.add(RelationType.getType(relation));
      }
    TimeRange tr = new TimeRange(st, et);
    try {
      queryrelationGraph =
          getNodeRelByIdInter(new Node(id), channel, attribute, getweight, tr, relationTypes);
    } catch (GdbException e) {
      return null;
    } catch (Exception e1) {
      e1.printStackTrace();
      return null;
    }
    Set<String> totalRelationTypes = null;
    List<NodeStatistic> nodeStatistic = null;
    if (relInfo) {
      totalRelationTypes = new HashSet<String>();
      for (Edge edge : queryrelationGraph.getCenterEdges()) {
        for (Relation relation : edge.getRelations())
          totalRelationTypes.add(relation.getType().getStringForm());
      }
    }
    if (queryrelationGraph == null)
      return null;

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
      return null;

    }
    GetRelationByNodeIdBean bean = new GetRelationByNodeIdBean();
    bean.addRelationGraph(relationGraph);
    bean.setRelationTypeList(totalRelationTypes);
    bean.setTotal(queryrelationGraph.getCenterEdges().size());
    bean.setNodeStatistic(nodeStatistic);
    return bean;
  }

  public static RelationGraph getNodeRelByIdInter(Node node, Channel channel, Attribute type,
      boolean getweight, TimeRange tr, Collection<RelationType> relTypes) throws GdbException {
    RelationGraph relationGraph = null;
    NodeType nodeType = null;
    // if (type != null) {
    // 取实体，对不是实体进行过滤（category 和identity）
    // if (type.equals("entity")) nodeType = NodeType.ANY_NODE;
    // else nodeType =
    // NodeType.getType(StrTypeMapper.ConvertToIntType(type));
    // } else nodeType = NodeType.ANY_NODE;
    // TimeRange tr=new TimeRange(0,System.currentTimeMillis()/1000);
    RelQuerySpec.RelQuerySpecBuilder specBuilder =
        new RelQuerySpec.RelQuerySpecBuilder(node).attribute(type).timeRange(tr);
    for (RelationType relType : relTypes)
      specBuilder.relType(relType);
    specBuilder.resultSize(Integer.MAX_VALUE);
    // specBuilder.timeRange(tr);
    if (getweight)
      specBuilder.useRelRank(true);// 采用排序接口,resultsize 默认为10000需不需要调整？
    relationGraph = adaGdbService.queryRelationGraph(specBuilder.build());
    if (type != null && type.equals("entity")) { // 对非实体进行删除（CATEGORY 和
      // IDENTITY）[新版里还有entity这种类型么Attribute.ENTITY]
      ArrayList<Edge> edgeToDel = new ArrayList<Edge>();
      for (Edge edge : relationGraph.getCenterEdges()) {
        if (edge.getTail().getType().getAttribute().equals(Attribute.CATEGORY))
          edgeToDel.add(edge);
      }
      for (Edge edge2 : edgeToDel)
        relationGraph.removeCenterEdgeAndRelatedOuterNode(edge2);
    }

    return relationGraph;
  }

  private static void addNodeNameForRelationGraph(RelationGraph graph) throws GdbException {
    addNodeNameForNodes(graph.getOuterNodes());
  }

  public static GetTowLevelRelationGraphBean getTwoLevelRelationGraph(String nodeId, boolean weight) {
    byte[] id = null;
    String ret = null;
    try {
      id = NodeIdConveter.checkAndtoBytes(nodeId);
    } catch (Exception e) {
      return null;
    }
    long s1 = System.currentTimeMillis();
    // 1 获取第一层节点的直接关系节点
    Node seedNode = new Node(id);
    RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(seedNode);
    relbuilder.useRelRank(weight).resultSize(100);
    RelationGraph relationGraph = null;
    try {
      relationGraph = adaGdbService.queryRelationGraph(relbuilder.build());
    } catch (GdbException e) {
      return null;
    }
    // 2 批量获取第二层节点的直接关系节点
    List<RelQuerySpec> specList = new ArrayList<RelQuerySpec>();
    for (Node node : relationGraph.getOuterNodes()) {
      RelQuerySpec.RelQuerySpecBuilder relbuilder1 = new RelQuerySpec.RelQuerySpecBuilder(node);
      relbuilder1.useRelRank(weight).resultSize(100);
      specList.add(relbuilder1.build());
    }
    int batchSize = specList.size() / (16 * 2) + 1;// 开多少个线程合适呢？现在暂定为32？？？？ 数据集中存在于edgeId表，开多线程意义大吗？
    List<RelationGraph> relationGraphList = null;
    try {
      relationGraphList = adaGdbService.queryRelationGraphsInParallel(specList, batchSize);
    } catch (GdbException e) {
      return null;
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
      return null;
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
        else
          iter.next();
    }
    return bean;
  }

}
