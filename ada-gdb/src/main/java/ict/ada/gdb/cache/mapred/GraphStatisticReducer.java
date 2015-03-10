package ict.ada.gdb.cache.mapred;

import ict.ada.common.util.Hex;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * Use reducers to aggregate all data for one Node, which may be in multiple HBase Regions.
 * 
 * node id, stat info ==> aggregated stat info line
 * 
 */
public class GraphStatisticReducer extends
    Reducer<ImmutableBytesWritable, Text, Text, NullWritable> {

  private static final String SET_ELE_SEP = GraphStatisticMapper.SET_ELE_SEP;

  private static final Pattern fieldRegx = Pattern.compile("\t");
  private static final Pattern setEleRegx = Pattern.compile(SET_ELE_SEP);

  private static final int NODEID_LEN = 18;
  /** overhead for each Cache kv, from pointers and alignment */
  private static final int KV_OVERHEAD = 8 * 3;// TODO

  private static final int KEY_LIST_OVERHEAD_PER_KEY = 2;// precise
  private static final int VALUE_DETAIL_OVERHEAD = 1 + 2 + 1 + 1;// pb related. not precise

  private static final int KEY_TYPE = 1;// key-type byte in Cache key

  private static final int NT_MEMCOST = 1;// node type byte in Cache key
  private static final int NODEID_MEMCOST = NODEID_LEN;

  private NumberFormat format;

  private static void addStringToSet(String[] strs, Set<String> set) {
    for (String s : strs) {
      set.add(s);
    }
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    format = NumberFormat.getNumberInstance();
    format.setMaximumFractionDigits(2);
    format.setMinimumFractionDigits(2);
  }

  @Override
  protected void reduce(ImmutableBytesWritable nodeid, Iterable<Text> stats, Context ctx)
      throws IOException, InterruptedException {
    int outDegree = 0;
    int weightSum = 0;
    int entryCount = 0;
    int relCnt = 0, relLenSum = 0;
    int ntCnt = 0;
    int ntRelCnt = 0, ntRelLenSum = 0;

    Set<String> relSet = new HashSet<String>();
    Set<String> nodeTypeSet = new HashSet<String>();
    Set<String> nodeTypeRelSet = new HashSet<String>();

    for (Text text : stats) {// aggregate each partition's statistics for one node
      String[] fields = fieldRegx.split(text.toString());
      outDegree += Integer.parseInt(fields[0]);
      weightSum += Integer.parseInt(fields[1]);
      entryCount += Integer.parseInt(fields[2]);
      String relSetLine = fields[3];
      if (relSetLine.length() != 0) {
        addStringToSet(setEleRegx.split(relSetLine), relSet);
      }
      String nodeTypeSetLine = fields[4];
      if (nodeTypeSetLine.length() != 0) {
        addStringToSet(setEleRegx.split(nodeTypeSetLine), nodeTypeSet);
      }
      String nodeTypeRelSetLine = fields[5];
      if (nodeTypeRelSetLine.length() != 0) {
        addStringToSet(setEleRegx.split(nodeTypeRelSetLine), nodeTypeRelSet);
      }
    }
    relCnt = relSet.size();
    for (String rel : relSet) {
      relLenSum += rel.length() / 2;// 2 for hex encoding
    }
    ntCnt = nodeTypeSet.size();
    ntRelCnt = nodeTypeRelSet.size();
    for (String ntRel : nodeTypeRelSet) {
      ntRelLenSum += ntRel.length() / 2;
    }

    int kvCnt = ntRelCnt + relCnt + ntCnt + 1;
    int memCost = 0;
    // type=0 kv, ntRelCnt kvs in total
    int type0KeyCost = (NODEID_MEMCOST + KEY_TYPE) * ntRelCnt + ntRelLenSum;// key
    memCost += type0KeyCost;
    memCost += VALUE_DETAIL_OVERHEAD * ntRelCnt; // value overhead
    memCost += (NODEID_MEMCOST + 1) * entryCount;// value payload, "1" for each weight
    // type=1 kv, ntCnt kvs in total
    memCost += (NODEID_MEMCOST + KEY_TYPE + NT_MEMCOST) * ntCnt;// key
    memCost += KEY_LIST_OVERHEAD_PER_KEY * ntRelCnt + type0KeyCost; // value
    // type=2 kv, relCnt kvs in total
    memCost += (NODEID_MEMCOST + KEY_TYPE) * relCnt + relLenSum;// key
    memCost += KEY_LIST_OVERHEAD_PER_KEY * ntRelCnt + type0KeyCost; // value
    // type=3 kv, only 1 kv
    memCost += NODEID_MEMCOST + KEY_TYPE;// key
    memCost += KEY_LIST_OVERHEAD_PER_KEY * ntRelCnt + type0KeyCost; // value
    // cache overhead
    memCost += KV_OVERHEAD * kvCnt;
    // memCost finish

    double weightAvg = weightSum * 1.0 / entryCount;

    String nodeIdHex = Hex.encodeHex(nodeid.get());
    String outputLine = nodeIdHex + "\t" + //
        outDegree + "\t" + //
        memCost + "\t" + //
        entryCount + "\t" + //
        ntRelCnt + "\t" + //
        ntRelLenSum + "\t" + //
        relCnt + "\t" + //
        relLenSum + "\t" + //
        ntCnt + "\t" + //
        format.format(weightAvg); //

    ctx.write(new Text(outputLine), NullWritable.get());
  }
}
