package ict.ada.gdb.cache.mapred;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.util.ByteArray;
import ict.ada.common.util.Hex;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

/**
 * 
 * hbase table regions ==> node id, partial stat info
 * 
 */
public class GraphStatisticMapper extends TableMapper<ImmutableBytesWritable, Text> {

  private static final int HASH_LEN = 1;
  private static final int NODEID_LEN = 18;

  byte[] curNodeId = null;
  private int outDegree = 0;
  private int entryCount = 0;// nt-id-nt-id-rel-weight count
  private int weightSum = 0;
  private Set<ByteArray> relSet = new HashSet<ByteArray>();
  private Set<Attribute> nodeTypeSet = new HashSet<NodeType.Attribute>();
  private Set<ByteArray> nodeTypeRelSet = new HashSet<ByteArray>();

  public static final String SET_ELE_SEP = ";";

  private String byteArraySetToString(Set<ByteArray> set) {
    StringBuilder sb = new StringBuilder();
    for (ByteArray arr : set) {
      sb.append(Hex.encodeHex(arr.getBytes()) + SET_ELE_SEP);
    }
    return sb.toString();
  }

  private String attrSetToString(Set<Attribute> set) {
    StringBuilder sb = new StringBuilder();
    for (Attribute nt : set) {
      sb.append(nt.getIntForm() + SET_ELE_SEP);
    }
    return sb.toString();
  }

  private String emitStatLine() {
    String relSetLine = byteArraySetToString(relSet);
    String nodeTypeSetLine = attrSetToString(nodeTypeSet);
    String nodeTypeRelSetLine = byteArraySetToString(nodeTypeRelSet);

    String line = outDegree + "\t" + //
        weightSum + "\t" + //
        entryCount + "\t" + //
        relSetLine + "\t" + //
        nodeTypeSetLine + "\t" + //
        nodeTypeRelSetLine;
    return line;
  }

  private void resetStat() {
    outDegree = 0;
    entryCount = 0;
    weightSum = 0;
    relSet.clear();
    nodeTypeSet.clear();
    nodeTypeRelSet.clear();
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result row, Context context) throws IOException,
      InterruptedException {
    byte[] rowkey = row.getRow();
    byte[] headId = new byte[NODEID_LEN];
    System.arraycopy(rowkey, HASH_LEN, headId, 0, NODEID_LEN);
    if (curNodeId == null) {// once
      curNodeId = headId;
    }
    if (Bytes.compareTo(curNodeId, headId) != 0) {// run into a new Node
      String v = emitStatLine();// print stat for previous node
      context.write(new ImmutableBytesWritable(curNodeId), new Text(v));
      resetStat();
      curNodeId = headId;
    }
    outDegree++;
    byte[] tailId = Bytes.tail(rowkey, NODEID_LEN);
    Attribute nt = NodeType.getType(tailId[0], tailId[1]).getAttribute();
    nodeTypeSet.add(nt);
    for (KeyValue kv : row.raw()) {// each kv for a rel-weight entry
      entryCount++;
      byte[] relBytes = kv.getQualifier();
      relSet.add(new ByteArray(relBytes));
      nodeTypeRelSet.add(new ByteArray(Bytes.add(nt.getByteFrom(), relBytes)));
      weightSum += Bytes.toLong(kv.getValue());
    }
  }

  public static void main(String[] args) {
    String s = "\t \t\t \t\t";
    System.out.println(s.split("\t").length);
  }
}
