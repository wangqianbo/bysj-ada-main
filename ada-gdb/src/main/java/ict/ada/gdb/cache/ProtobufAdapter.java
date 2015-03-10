package ict.ada.gdb.cache;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.RelationGraph;
import ict.ada.common.model.RelationType;
import ict.ada.common.util.Hex;
import ict.ada.gdb.cache.GdbCacheProto.CacheExploreSpec;
import ict.ada.gdb.cache.GdbCacheProto.EdgeList;
import ict.ada.gdb.cache.GdbCacheProto.EdgeRelInfo;
import ict.ada.gdb.common.PathQuerySpec;
import ict.ada.gdb.common.RelQuerySpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;

/**
 * Conversions between Protocol Buffer types and java types.
 */
public class ProtobufAdapter {

  /*
   * TODO RelQuerySepc can have a list of node type
   */

  /**
   * from RelQuerySpec to Protobuf's CacheExploreSpec.
   * Used in GDB client's jvm
   */
  public static CacheExploreSpec toProtobuf(RelQuerySpec spec, boolean loadCacheOnMiss) {
    CacheExploreSpec.Builder builder = CacheExploreSpec.newBuilder();
    builder.setCenterId(ByteString.copyFrom(spec.getCenterNode().getId()));
    // result size
    builder.setResultSize(spec.getResultSize());
    // node attribute type
    Attribute nodeAttr = spec.getRequiredAttribute();
    // ANY is represented by nonexistence of nodeAttrType field in PB message
    if (nodeAttr != Attribute.ANY) {
      builder.addNodeAttrType(spec.getRequiredAttribute().getIntForm());
    }
    // required relation types
    Set<RelationType> relTypes = spec.getRequiredRelType();
    if (relTypes != null) {
      for (RelationType t : relTypes) {
        builder.addRequiredRelType(t.getStringForm());
      }
    }
    // useRelRank
    builder.setUseWeightRank(spec.isRelRankEnabled());
    // min/max edge weight
    builder.setWeightMin(spec.getEdgeWeightMin());
    builder.setWeightMax(spec.getEdgeWeightMax());

    // load cache on miss
    builder.setLoadCacheOnMiss(loadCacheOnMiss);

    return builder.build();
  }

  public static CacheExploreSpec toProtobuf(Node center, PathQuerySpec spec, boolean loadCacheOnMiss) {
    CacheExploreSpec.Builder builder = CacheExploreSpec.newBuilder();
    builder.setCenterId(ByteString.copyFrom(center.getId()));
    Attribute nodeAttr = spec.getRequiredAttribute();
    if (nodeAttr != Attribute.ANY) {
      builder.addNodeAttrType(spec.getRequiredAttribute().getIntForm());
    }
    builder.setUseWeightRank(false);// ranking is not important in PathQuery
    builder.setLoadCacheOnMiss(loadCacheOnMiss);

    return builder.build();
  }

  // /**
  // * from Protobuf's CacheExploreSpec to RelQuerySpec.
  // * Used in GdbCache's jvm.
  // */
  // public static RelQuerySpec fromProtobuf(CacheExploreSpec pbSpec) {
  // byte[] centerId = pbSpec.getCenterId().toByteArray();
  // Node centerNode = new Node(centerId);
  // RelQuerySpecBuilder builder = new RelQuerySpecBuilder(centerNode);
  // // result size
  // if (pbSpec.hasResultSize()) {
  // builder.resultSize(pbSpec.getResultSize());
  // }
  // // node type
  // if (pbSpec.hasNodeAttrType()) {
  // builder.attribute(Attribute.getAttribute(pbSpec.getNodeAttrType()));
  // }
  // // relation types
  // if (pbSpec.getRequiredRelTypeCount() > 0) {
  // for (String relType : pbSpec.getRequiredRelTypeList()) {
  // builder.relType(RelationType.getType(relType));
  // }
  // }
  // return builder.build();
  // }

  /**
   * Construct RelationGraph from EdgeList.
   * Used in GDB client's JVM to interpret RelQuery result from GDB Cache
   * 
   * @param raw
   *          query result raw Protocol Buffer data from GDB cache
   */
  public static RelationGraph convertEdgeListPB(RelQuerySpec spec, EdgeList raw) {
    byte[] centerId = raw.getStartId().toByteArray();
    if (Bytes.compareTo(centerId, spec.getCenterNode().getId()) != 0) {
      throw new IllegalStateException("Returned center id(" + Hex.encodeHex(centerId)
          + ") is not the same with node id in RelQuerySpec("
          + Hex.encodeHex(spec.getCenterNode().getId()) + "). GdbCache BUG ? ");
    }
    Node centerNode = new Node(centerId);
    RelationGraph relGraph = new RelationGraph(centerNode);
    int edgeCount = raw.getEdgeRelInfoCount();
    if (edgeCount > spec.getResultSize()) {// sanity check
      throw new IllegalStateException("Returned result size is " + edgeCount
          + ", larger than required " + spec.getResultSize());
    }
    if (edgeCount == 0) return relGraph;
    for (EdgeRelInfo relsOnEdge : raw.getEdgeRelInfoList()) {// for each Edge
      Node tailNode = new Node(relsOnEdge.getTailId().toByteArray());
      Edge edge = new Edge(centerNode, tailNode);
      if (relsOnEdge.getRelTypeCount() != relsOnEdge.getRelWeightCount()) {// sanity check
        throw new IllegalStateException("Should be equal. RelType count="
            + relsOnEdge.getRelTypeCount() + " RelWeight count=" + relsOnEdge.getRelWeightCount());
      }
      for (int seq = 0; seq < relsOnEdge.getRelTypeCount(); ++seq) {
        edge.addRelation(RelationType.getType(relsOnEdge.getRelType(seq)),
            relsOnEdge.getRelWeight(seq));
      }
      relGraph.addCenterEdge(edge);
    }
    return relGraph;
  }

  public static List<byte[]> convertEdgeListPB(Node nodeToExpand, PathQuerySpec spec, EdgeList raw) {
    byte[] centerId = raw.getStartId().toByteArray();
    if (Bytes.compareTo(nodeToExpand.getId(), centerId) != 0) {
      throw new IllegalStateException("Returned center id(" + Hex.encodeHex(centerId)
          + ") is not the same with NodeToExpand(" + Hex.encodeHex(nodeToExpand.getId())
          + "). GdbCache BUG ? ");
    }
    int edgeCount = raw.getEdgeRelInfoCount();
    if (edgeCount == 0) return Collections.emptyList();

    List<byte[]> relatedIdList = new ArrayList<byte[]>(edgeCount);
    for (EdgeRelInfo relsOnEdge : raw.getEdgeRelInfoList()) {// for each Edge
      byte[] tailId = relsOnEdge.getTailId().toByteArray();
      relatedIdList.add(tailId);
    }
    return relatedIdList;

  }

}
