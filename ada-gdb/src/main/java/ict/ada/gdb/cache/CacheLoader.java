package ict.ada.gdb.cache;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.common.util.Hex;
import ict.ada.common.util.Pair;
import ict.ada.common.util.Timer;
import ict.ada.gdb.cache.GdbCacheProto.CacheValueDetail;
import ict.ada.gdb.cache.GdbCacheProto.KeyValueList;
import ict.ada.gdb.common.AdaConfig;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.AdaModeConfig.GDBMode;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.dao.HBaseDAOFactory;
import ict.ada.gdb.dao.HBaseEdgeDAO;

public class CacheLoader {

  static {
    AdaModeConfig.setMode(GDBMode.QUERY);
  }

  private static final HBaseEdgeDAO edgeDAO = HBaseDAOFactory.getHBaseEdgeDAO(false);

  private static final int NODE_ID_SIZE = Node.NODEID_SIZE;
  private static final byte KV_TYPE_VALUE = 0;
  private static final byte KV_TYPE_SIZE = 1;
  private static final byte NODE_ATTR_SIZE = 1;

  // TODO if buildKeyValueListForOneNode() is not efficient enough for JNI, process multiple nodes
  // in one call.

  /**
   * Called through JNI by Gdb Cache.
   * Get all relation data for a given Node and build Protobuf KeyValueList for Gdb Cache.
   * Keys in KeyValueList is with type=0, values in KeyValueList are CacheValueDetail PB bytes.
   * 
   * @see see comments in Protobuf definition file for more.
   * 
   * @param nodeid
   * @return PB bytes of KeyValueList
   * @throws GdbException
   */
  public static byte[] buildKeyValueListForOneNode(byte[] nodeid) throws GdbException {
    if (nodeid == null) {
      log("null node id");
      return null;
    }
    if (nodeid.length != NODE_ID_SIZE) {
      log("Invalid Node id: " + Hex.encodeHex(nodeid));
      return null;
    }
    long start = Timer.now();
    Map<String, Map<Attribute, List<Pair<byte[], Integer>>>> nodeData = edgeDAO
        .queryOneNodeContentForCache(nodeid);
    System.out.println("EdgeDAO time cost: " + Timer.msSince(start) + "ms");

    KeyValueList.Builder kvListBuilder = KeyValueList.newBuilder();
    for (Entry<String, Map<Attribute, List<Pair<byte[], Integer>>>> relE : nodeData.entrySet()) {
      String relStr = relE.getKey();
      for (Entry<Attribute, List<Pair<byte[], Integer>>> attrE : relE.getValue().entrySet()) {
        byte[] attrByte = attrE.getKey().getByteFrom();
        // Build a Cache Key-Value entry
        ByteString kvKey = buildCacheKey(nodeid, attrByte[0], relStr);
        ByteString kvValue = buildCacheValue(attrE.getValue());
        kvListBuilder.addKey(kvKey).addValue(kvValue);
      }
    }
    byte[] result = kvListBuilder.build().toByteArray();
    if (result.length > 5 * 1024 * 1024) {
      log("Warning! Large result size:" + result.length + " Node ID:" + Hex.encodeHex(nodeid));
    }
    return result;
  }

  private static ByteString buildCacheKey(byte[] nodeid, byte attrByte, String relStr) {
    byte[] relStrBytes = Bytes.toBytes(relStr);
    // Key format:|-- 18B Node id --|-- 1B KV type --|-- 1B Node type --|-- rel string --|
    byte[] kvKey = new byte[NODE_ID_SIZE + KV_TYPE_SIZE + NODE_ATTR_SIZE + relStrBytes.length];
    System.arraycopy(nodeid, 0, kvKey, 0, NODE_ID_SIZE);
    kvKey[NODE_ID_SIZE] = KV_TYPE_VALUE;
    kvKey[NODE_ID_SIZE + KV_TYPE_SIZE] = attrByte;
    System.arraycopy(relStrBytes, 0, kvKey, NODE_ID_SIZE + KV_TYPE_SIZE + NODE_ATTR_SIZE,
        relStrBytes.length);
    // System.out.println("CacheLoader Key:" + Hex.encodeHex(kvKey));
    return ByteString.copyFrom(kvKey);
  }

  private static ByteString buildCacheValue(List<Pair<byte[], Integer>> nodeIdWeightPairList) {
    CacheValueDetail.Builder valueBuilder = CacheValueDetail.newBuilder();
    int pairCnt = nodeIdWeightPairList.size();
    byte[] packedNodeId = new byte[pairCnt * NODE_ID_SIZE];// pack node ids into one array
    for (int idx = 0; idx < pairCnt; ++idx) {
      Pair<byte[], Integer> pair = nodeIdWeightPairList.get(idx);
      System.arraycopy(pair.getFirst(), 0, packedNodeId, idx * NODE_ID_SIZE, NODE_ID_SIZE);
      valueBuilder.addWeight(pair.getSecond());// weight order is important!
    }
    valueBuilder.setNodeIdList(ByteString.copyFrom(packedNodeId));
    CacheValueDetail valueMsg = valueBuilder.build();
    return valueMsg.toByteString();
  }

  private static void log(String log) {
    System.out.println("[JNI CacheLoader] " + log);
  }

  private static void test(String nodeidHex) throws GdbException {
    long start = Timer.now();
    byte[] result = buildKeyValueListForOneNode(Hex.decodeHex(nodeidHex));
    System.out.println("Time cost: " + Timer.msSince(start) + "ms");
    System.out.println("Result size=" + result.length);
  }

  public static void main(String[] args) throws GdbException {
    System.out.println(AdaConfig.CONNECTION_STRING);
    System.out.println(AdaModeConfig.getDBVersion(Channel.BBS));
    String xijinping = "01331fa2bf8c04ffcaf2a134736add6150c6";

    test(xijinping);
    test(xijinping);
    test(xijinping);
    test(xijinping);
    test(xijinping);
  }

}
