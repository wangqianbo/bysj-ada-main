package ict.ada.gdb.service;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Event;
import ict.ada.common.model.Node;
import ict.ada.common.model.NodeAttribute;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.common.model.PathGraph;
import ict.ada.common.model.Relation;
import ict.ada.common.model.RelationGraph;
import ict.ada.common.model.RelationType;
import ict.ada.common.util.Pair;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.PathQuerySpec;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.common.TimeRange;
import ict.ada.gdb.rowcounter.TableRowCount;

import java.util.List;
import java.util.Map;

/**
 * 
 * GDB Service Interface.<br>
 * Client code should use this class to interact with GDB.
 * 
 */
public class AdaGdbService {

  private InternalGraphDB internalGdb;

  /**
   * Default to AdaGdbService(GDBMode.INSERT)
   */
  public AdaGdbService() {
    internalGdb = new InternalGraphDB();
  }

  public AdaGdbService(AdaModeConfig.GDBMode mode) {
    internalGdb = new InternalGraphDB();
    AdaModeConfig.setMode(mode);
  }

  public byte[] addNode(Node node) throws GdbException {
    return internalGdb.addNode(node);
  }

  /**
   * Flush write cache( e.g. node name index cache)
   * Remember to call this after you finish adding Nodes.
   * 
   * @throws GdbException
   */
  public void finishUpdate() throws GdbException {
    internalGdb.finishUpdate();
  }

  public void addEdge(Edge edge) throws GdbException {
    internalGdb.addEdge(edge);
  }

  public void addEdgeAndNodes(Edge edge) throws GdbException {
    internalGdb.addEdgeAndNodes(edge);
  }

  public byte[] getNodeIdByName(String name, NodeType type) throws GdbException {
    return internalGdb.getNodeIdByName(name, type);
  }

  public String getNodeNameById(byte[] id, NodeType type) throws GdbException {
    return internalGdb.getNodeNameById(id, type);
  }

  public Node getNodeNameAndSnameById(byte[] id) throws GdbException {
    return internalGdb.getNodeNameAndSnameById(id);
  }

  public Node getNodeEntsById(byte[] id) throws GdbException{
	  return internalGdb.getNodeEntsById(id);
  }
  public List<Pair<String, List<String>>> getNodeNameAndSnameByIdBatched(List<byte[]> ids)
      throws GdbException {
    return internalGdb.getNodeNameAndSnameByIdBatched(ids);
  }

  public List<Pair<String, List<String>>> getNodeNameAndSnameByIdBatched(List<byte[]> ids,
      int batchSize) throws GdbException {
    return internalGdb.getNodeNameAndSnameByIdBatched(ids, batchSize);
  }

  public void deleteSearchNames(Channel channel) throws GdbException {
    internalGdb.deleteSearchNames(channel);
  }

  public Node getNodeAttributes(Node node,boolean loadInfo) throws GdbException {
    return internalGdb.getNodeAttributes(node,loadInfo);
  }

  public Node getNodeAttributes(Node node, List<byte[]> wdeIds) throws GdbException {
    return internalGdb.getNodeAttributes(node, wdeIds);
  }
  public List<Pair<Integer,NodeAttribute>> getNodeActionAttrWdeRefs(Node node,String value,TimeRange timeRange) throws GdbException{
		return internalGdb.getNodeActionAttrWdeRefs(node,value, timeRange);
	}
  public Node getNodeWdeRefs(Node node, TimeRange timeRange) throws GdbException {
    return internalGdb.getNodeWdeRefs(node, timeRange);
  }
  public List<NodeAttribute> getNodeAttrWdeRefs(Node node,List<NodeAttribute> attributes) throws GdbException{
	  return internalGdb.getNodeAttrWdeRefs(node, attributes);
  }
  public void rebuildIndexByChannel(Channel channel) throws GdbException{
	  internalGdb.rebuildIndexByChannel(channel);
  } 
  public Pair<List<Edge>, List<Edge>> getExistEdges(List<Edge> edges) throws GdbException {
    return internalGdb.getExistEdges(edges);
  }

  public Relation getRelationDetail(Relation rel) throws GdbException {
    return internalGdb.getRelationDetail(rel);
  }

  public List<Pair<Integer, List<Relation>>> getEdgeRelations(Edge edge,
      RelationType relTypeConstraint, TimeRange timeRange, boolean loadWdeRefs) throws GdbException {
    return internalGdb.getEdgeRelations(edge, relTypeConstraint, timeRange, loadWdeRefs);
  }

  public List<Edge> getEdgesRelations(List<Edge> edges, RelationType relTypeConstraint,
      TimeRange timeRange, boolean loadWdeRefs) throws GdbException {
    return internalGdb.getEdgesRelations(edges, relTypeConstraint, timeRange, loadWdeRefs);
  }

  public RelationGraph queryRelationGraph(RelQuerySpec spec) throws GdbException {
    return internalGdb.queryRelationGraph(spec);
  }

  public List<RelationGraph> queryRelationGraphsInParallel(List<RelQuerySpec> specList,
      int batchSize) throws GdbException {
    return internalGdb.queryRelationGraphsInParallel(specList, batchSize);
  }

  @Deprecated
  public RelationGraph queryRelationGraphWithOutNodeName(RelQuerySpec spec) throws GdbException {
    return internalGdb.queryRelationGraphWithOutNodeName(spec);
  }

  public PathGraph queryPathGraph(PathQuerySpec spec) throws GdbException {
    return internalGdb.queryPathGraph(spec);
  }

  public TableRowCount getTableRowCount() throws GdbException {
    return internalGdb.getTableRowCount();
  }

  public void deleteNode(Node node) throws GdbException {
    internalGdb.deleteNode(node);
  }

  public List<List<Node>> getNodeCommunityPersonRelList(Node node, String method)
      throws GdbException {
    return internalGdb.getNodeCommunityPersonRelList(node, method);
  }
  public void cleanNodeHTable(Channel channel){
	  internalGdb.cleanNodeHTable(channel);
}
  public void cleanRelationTypeHTableByTS(Channel channel,TimeRange timeRange){
	  internalGdb.cleanRelationTypeHTableByTS(channel, timeRange);
 }
 public void cleanEdgeIdAndEdgeSumTableByTs(Channel channel,TimeRange timeRange){
	 internalGdb.cleanEdgeIdAndEdgeSumTableByTs(channel, timeRange);
}

  public int getIndexNum(Channel channel) {
    return internalGdb.getIndexNum(channel);
  }

  public Map<String, byte[]> getRelationType(Channel channel)  {
    return internalGdb.getRelationType(channel);
  }
  
  public Map<String, byte[]> getRelationTypeV1(Channel channel) throws GdbException  {
    return internalGdb.getRelationTypeV1(channel);
  }
}
