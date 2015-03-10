package ict.ada.gdb.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.NodeAttribute;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.common.model.PathGraph;
import ict.ada.common.model.Relation;
import ict.ada.common.model.RelationGraph;
import ict.ada.common.model.RelationType;
import ict.ada.common.util.Pair;
import ict.ada.common.util.Timer;
import ict.ada.gdb.common.AdaConfig;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.PathQuerySpec;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.common.TimeRange;
import ict.ada.gdb.dao.HBaseAggregationDao;
import ict.ada.gdb.dao.HBaseDAOFactory;
import ict.ada.gdb.dao.HBaseEdgeDAO;
import ict.ada.gdb.dao.HBaseNodeDAO;
import ict.ada.gdb.rowcounter.TableRowCount;
import ict.ada.gdb.util.NodeIdConveter;
import ict.ada.gdb.util.ParallelTask;

/**
 * Define basic graph operations.<br>
 * AdaGdbService will provide a more high-level GDB interface based on these operations.
 * 
 */
public class InternalGraphDB {
  private static final Log LOG = LogFactory.getLog(InternalGraphDB.class);

  private HBaseNodeDAO nodeDAO;
  private HBaseEdgeDAO edgeDAO;
  private HBaseAggregationDao aggregationDao;
  
  private final ExecutorService exec;

  // private HBaseWdeDAO wdeDAO;

  public InternalGraphDB() {
    nodeDAO = HBaseDAOFactory.getHBaseNodeDAO();
    edgeDAO = HBaseDAOFactory.getHBaseEdgeDAO();
    aggregationDao = HBaseDAOFactory.getHBaseAggregationDao();
    
    // wdeDAO = HBaseDAOFactory.getHBaseWdeDAO();
    exec = Executors.newCachedThreadPool();
  }

  /*
   * Graph Writer
   */
  public byte[] addNode(Node node) throws GdbException {
    byte[] nodeId = null;
    nodeId = nodeDAO.addNode(node);
    return nodeId;
  }

  public void finishUpdate() throws GdbException {
    nodeDAO.finishUpdate();
  }

  /**
   * Add one directed Edge. <br>
   * If one Node in Edge has no id info, addNode() will be called to add the Node and THUS get the
   * Node id. Otherwise, addNode() will not be called for it.<br>
   * (The Nodes in Edge may not exist, so addNode() is called instead of getNodeIdByName().)
   * 
   * 
   * @param edge
   * @throws GdbException
   */
  public void addEdge(Edge edge) throws GdbException {
    if (ignoreEdge(edge)) return;
    if (edge.getHead().getId() == null) {
      this.addNode(edge.getHead());
    }
    if (edge.getTail().getId() == null) {
      this.addNode(edge.getTail());
    }
    // Now, Nodes in Edge should carry id information, which is essential for Edge addition below.
    edgeDAO.addDirectedEdge(edge);
  }

  /**
   * Add one directed Edge and its two Nodes.
   * 
   * @param edge
   * @throws GdbException
   */
  public void addEdgeAndNodes(Edge edge) throws GdbException {
    if (ignoreEdge(edge)) return;
    this.addNode(edge.getHead());
    this.addNode(edge.getTail());
    // Now, Nodes in Edge should carry id information, which is essential for Edge addition below.
    edgeDAO.addDirectedEdge(edge);
  }

  private boolean ignoreEdge(Edge edge) {
    if (edge == null) throw new IllegalArgumentException("null Edge");
    if (!AdaConfig.GRAPH_ACCEPT_SELFLOOP && edge.identicalHeadAndTail()) {
      LOG.info("Ignore one Edge with self loop.");
      return true;
    } else return false;
  }

  /*
   * Node Query
   */

  public Node getNodeAttributes(Node node,boolean loadInfo) throws GdbException {
    return nodeDAO.getNodeAttributes(node,loadInfo);
  }
  public Node getNodeAttributes(Node node,List<byte[]>wdeIds) throws GdbException {
    return nodeDAO.getNodeAttributes(node,wdeIds);
  }
  public Node getNodeWdeRefs(Node node, TimeRange timeRange) throws GdbException {
    return nodeDAO.getNodeWdeRefs(node, timeRange);
  }
  public List<NodeAttribute> getNodeAttrWdeRefs(Node node,List<NodeAttribute> attributes) throws GdbException{
	  return nodeDAO.getNodeAttrWdeRefs(node, attributes);
  }
public List<Pair<Integer,NodeAttribute>> getNodeActionAttrWdeRefs(Node node,String value,TimeRange timeRange) throws GdbException{
	return nodeDAO.getNodeActionAttrWdeRefs(node, value,timeRange);
}
  public byte[] getNodeIdByName(String name, NodeType type) throws GdbException {
    return nodeDAO.getNodeIdByName(name, type);
  }

  public String getNodeNameById(byte[] id, NodeType type) throws GdbException {
    return nodeDAO.getNodeNameById(id, type);
  }
  public Node getNodeEntsById(byte[] id) throws GdbException{
	  return nodeDAO.getNodeEntsById(id);
  }
  
  public List<Pair<String, List<String>>> getNodeNameAndSnameByIdBatched(List<byte[]> ids)
      throws GdbException {
    long start = Timer.now();
    List<Pair<String, List<String>>> result= nodeDAO.getNodeNameAndSnameByIdBatched(ids);
    LOG.info("NodeNameAndSnameByIdBatched: Get Names in " + Timer.msSince(start) + "ms");
    return result;
  } 
public List<Pair<String, List<String>>> getNodeNameAndSnameByIdBatched(List<byte[]> ids,int batchSize) throws GdbException{
  
  if (batchSize <= 0) throw new IllegalArgumentException("batchSize=" + batchSize);
  @SuppressWarnings("unchecked")
  final Pair<String, List<String>>[] results = new Pair[ids.size()];
  ParallelTask<List<Pair<Integer, Pair<String, List<String>>>>> task = new ParallelTask<List<Pair<Integer, Pair<String, List<String>>>>>(
      exec) {
    @Override
    public void processResult(List<Pair<Integer, Pair<String, List<String>>>> pairs) {
      for (Pair<Integer, Pair<String, List<String>>> p : pairs) {
        results[p.getFirst()] = p.getSecond();
      }
    }
  };
  for (int batchStart = 0; batchStart < ids.size(); batchStart += batchSize) {
    final int curBatchSize = (batchStart + batchSize > ids.size()) ? ids.size()
        - batchStart : batchSize;
    final List<byte[]> curBatchIds = ids.subList(batchStart, batchStart
        + curBatchSize);
    final int curBatchStart = batchStart;
    // each Thread handles curBatchSize specs
    task.submitTasks(new Callable<List<Pair<Integer, Pair<String, List<String>>>>>() {
      @Override
      public List<Pair<Integer, Pair<String, List<String>>>> call() throws GdbException {
        List<Pair<Integer, Pair<String, List<String>>>> batchResult = new ArrayList<Pair<Integer, Pair<String, List<String>>>>(
            curBatchSize);
        List<Pair<String, List<String>>> curBatchResults =getNodeNameAndSnameByIdBatched(curBatchIds);
        int i=0;
        for(Pair<String, List<String>> result:curBatchResults)
          batchResult.add(new Pair<Integer,Pair<String, List<String>>>(curBatchStart + i++, result));
        return batchResult;
      }
    });
  }
  try {
    task.gatherResults();
  } catch (Exception e) {
    throw new GdbException("Fail to query nodeNames in parallel", e);
  }
  return Arrays.asList(results);

}
   
  public void deleteSearchNames(Channel channel) throws GdbException {
    nodeDAO.deleteSearchNamesInParallel(channel);
  }

  public Node getNodeNameAndSnameById(byte[] id) throws GdbException {
    return nodeDAO.getNodeNameAndSnameById(id);
  }

  public void rebuildIndexByChannel(Channel channel) throws GdbException{
	  nodeDAO.rebuildIndex(channel);
  } 
  /*
   * Edge/Relation Query
   */

  public Pair<List<Edge>, List<Edge>> getExistEdges(List<Edge> edges) throws GdbException{
    return edgeDAO.getExistEdges(edges);
  }
  public Relation getRelationDetail(Relation rel) throws GdbException {
    return edgeDAO.getRelationDetail(rel);
  }

  public List<Pair<Integer, List<Relation>>>  getEdgeRelations(Edge edge, RelationType relTypeConstraint, TimeRange timeRange,
      boolean loadWdeRefs) throws GdbException {
    // TODO enumSet for multiple RelTypeConstraints?
    return edgeDAO.getEdgeRelations(edge, relTypeConstraint, timeRange, loadWdeRefs);
  }
  public List<Edge> getEdgesRelations(List<Edge> edges, RelationType relTypeConstraint, TimeRange timeRange,
      boolean loadWdeRefs) throws GdbException {
    // TODO enumSet for multiple RelTypeConstraints?
    return edgeDAO.getEdgesRelations(edges, relTypeConstraint, timeRange, loadWdeRefs);
  }
  /**
   * 
   * @param specList
   * @param batchSize
   *          query "batchSize" RelationGraphs in each Thread. If batchSize=1, one Thread for each
   *          RelationGraph
   * @return corresponding RelationGraph for each spec in given specList
   * @throws GdbException
   */
  public List<RelationGraph> queryRelationGraphsInParallel(final List<RelQuerySpec> specList,
      int batchSize) throws GdbException {
    if (batchSize <= 0) throw new IllegalArgumentException("batchSize=" + batchSize);
    final RelationGraph[] results = new RelationGraph[specList.size()];
    ParallelTask<List<Pair<Integer, RelationGraph>>> task = new ParallelTask<List<Pair<Integer, RelationGraph>>>(
        exec) {
      @Override
      public void processResult(List<Pair<Integer, RelationGraph>> pairs) {
        for (Pair<Integer, RelationGraph> p : pairs) {
          results[p.getFirst()] = p.getSecond();
        }
      }
    };
    for (int batchStart = 0; batchStart < specList.size(); batchStart += batchSize) {
      final int curBatchSize = (batchStart + batchSize > specList.size()) ? specList.size()
          - batchStart : batchSize;
      final List<RelQuerySpec> curBatchSpecs = specList.subList(batchStart, batchStart
          + curBatchSize);
      final int curBatchStart = batchStart;
      // each Thread handles curBatchSize specs
      task.submitTasks(new Callable<List<Pair<Integer, RelationGraph>>>() {
        @Override
        public List<Pair<Integer, RelationGraph>> call() throws GdbException {
          List<Pair<Integer, RelationGraph>> batchResult = new ArrayList<Pair<Integer, RelationGraph>>(
              curBatchSize);
          for (int i = 0; i < curBatchSpecs.size(); i++) {
            RelationGraph graph = queryRelationGraphWithOutNodeName(curBatchSpecs.get(i));
            // first field of Pair is the index for RelationGraph in the final result list
            batchResult.add(new Pair<Integer, RelationGraph>(curBatchStart + i, graph));
          }
          return batchResult;
        }
      });
    }
    try {
      task.gatherResults();
    } catch (Exception e) {
      throw new GdbException("Fail to query RelationGraphs in parallel", e);
    }
    return Arrays.asList(results);
  }

  public RelationGraph queryRelationGraph(RelQuerySpec spec) throws GdbException {// TODO
                                                                                  // 这个接口还需要测试，针对多通道的情况
    // TODO multiple relation types?
    long start = Timer.now();
    RelationGraph graph = edgeDAO.queryRelationGraph(spec);
    LOG.info("RelationQuery: Get RelationGraph in " + Timer.msSince(start) + "ms");
    return graph;
  }
  

  public RelationGraph queryRelationGraphWithOutNodeName(RelQuerySpec spec) throws GdbException {
    // TODO multiple relation types?
    long start = Timer.now();
    RelationGraph graph = edgeDAO.queryRelationGraph(spec);
    LOG.info("RelationQuery: Get RelationGraph in " + Timer.msSince(start) + "ms");
    return graph;
  }

  public RelationGraph queryRelationGraphTest(RelQuerySpec spec) throws GdbException {
    // TODO multiple relation types?
    long start = Timer.now();
    RelationGraph graph = edgeDAO.queryRelationGraph(spec);
    LOG.info("RelationQuery: Get RelationGraph in " + Timer.msSince(start) + "ms");

    start = Timer.now();
    for (Node node : graph.getOuterNodes()) {
      Node node1 = nodeDAO.getNodeNameAndSnameById(node.getId());
      node.getType();
      if (node1.getName() == null) {
        System.out.println(NodeIdConveter.toString(node1.getId()));
        continue;
      }
      node.setName(node1.getName());
      for (String sname : node1.getSnames())
        node.addSearchName(sname);
    }
    LOG.info("RelationQuery: Get Node names in " + Timer.msSince(start) + "ms");
    return graph;
  }

  public PathGraph queryPathGraph(PathQuerySpec spec) throws GdbException {
    return edgeDAO.queryPathGraph(spec);
  }
  
  
  public TableRowCount getTableRowCount() throws GdbException{
    return aggregationDao.getTableRowCount();
  }
  
  
  public List<List<Node>> getNodeCommunityPersonRelList(Node node,String method) throws GdbException{
    return nodeDAO.getNodeCommunityPersonRelList(node,method);
  }
  public void deleteNode(Node node) throws GdbException{
    String exceptionMassage="";
    try {
      nodeDAO.deleteNode(node);
    } catch (GdbException e) {
      exceptionMassage+=e.getMessage();
    }
   try{
     edgeDAO.deleteEdge(node);
   }catch(Exception e){
    exceptionMassage+=e.getMessage();
   }
   if(!exceptionMassage.equals(""))
     throw new GdbException(exceptionMassage);
  }
  
  
  public int getIndexNum(Channel channel){
	  return AdaModeConfig.getIndexNumber(channel);
  }
  public void cleanNodeHTable(Channel channel){
	   nodeDAO.cleanNodeHTable(channel);
 }
  public void cleanRelationTypeHTableByTS(Channel channel,TimeRange timeRange){
	   edgeDAO.cleanRelationTypeHTableByTS(channel, timeRange);
  }
  public void cleanEdgeIdAndEdgeSumTableByTs(Channel channel,TimeRange timeRange){
	   edgeDAO.cleanEdgeIdAndEdgeSumTableByTs(channel, timeRange);
 }
  public Map<String,byte[]> getRelationType(Channel channel) {
    return edgeDAO.getRelationType(channel);
  }
  public Map<String,byte[]> getRelationTypeV1(Channel channel) throws GdbException{
	  return edgeDAO.getRelationTypes(channel);
  }
}
