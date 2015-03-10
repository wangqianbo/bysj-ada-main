package ict.ada.gdb.coprocessor;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.RelationType;
import ict.ada.common.model.WdeRef;
import ict.ada.gdb.common.DelEdge;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.common.RelQuerySpecWritable;
import ict.ada.gdb.common.SortedWdeRefSet;
import ict.ada.gdb.schema.EdgeIdHTable;
import ict.ada.gdb.schema.EdgeRelWeightDetailHTable;
import ict.ada.gdb.schema.EdgeRelWeightSumHTable;
import ict.ada.gdb.schema.GdbHTableType;
import ict.ada.gdb.schema.RelationWdeRefsHTable;
import ict.ada.gdb.util.Pair;
import ict.ada.gdb.util.ParallelTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

//Aggregation implementation at a region.
public class HBaseEdgeDaoEndPoint extends BaseEndpointCoprocessor implements HBaseEdgeDaoProtocol {
  /**
   * Logger for this class
   */

  private static final Log LOG = LogFactory.getLog(HBaseEdgeDaoEndPoint.class);
  private final ExecutorService exec = new ThreadPoolExecutor(40, Integer.MAX_VALUE, 60L,
      TimeUnit.SECONDS, new SynchronousQueue<Runnable>());// core pool size = 40
  private static final int SALT_PARTITION_COUNT = 32;// TODO determine table scalability
  private static final int SALT_PREFIX_BYTE_SZIE = 1;// one byte salt prefix
  private static final int BATCH_SIZE = 1000;
  private static final byte[] EMPTY_QUALIFIER = new byte[0];

  @Override
  public long sum(byte[] family, byte[] qualifier) throws IOException {
    // aggregate at each region
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    long sumResult = 0;
    RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) getEnvironment();
    InternalScanner scanner = environment.getRegion().getScanner(scan);
    try {
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean hasMore = false;
      do {
        curVals.clear();
        hasMore = scanner.next(curVals);
        KeyValue kv = curVals.get(0);
        sumResult += Bytes.toLong(kv.getValue());
      } while (hasMore);
    } finally {
      scanner.close();
    }
    return sumResult;
  }

  @Override
  public Result queryRelGraphRelationWdeRefsHTable(RelQuerySpecWritable specWritable)
      throws IOException {// test may cost too many cache in server (use batch)
    try {
      RelQuerySpec spec = specWritable.getRelQuerySpec();
      Set<ByteArray> wdeIdSet = spec.getContainedWdeIds();// 有可能是null
      Node centerNode = spec.getCenterNode();
      byte[] familiy = RelationWdeRefsHTable.FAMILY;
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      Scan scan = new Scan();
      scan.setBatch(Integer.MAX_VALUE);
      byte[] scanPrefix = Bytes.add(centerNode.getId(), spec.getRequiredAttribute().getByteFrom());
      Filter filter = new PrefixFilter(scanPrefix);
      scan.setFilter(filter).setCaching(1000);// TOD
      scan.setStartRow(scanPrefix);// for performance
      InternalScanner scanner = null;
      RegionCoprocessorEnvironment environment1 = (RegionCoprocessorEnvironment) getEnvironment();
      scanner = environment1.getRegion().getScanner(scan);
      Set<RelationType> reqRelType = spec.getRequiredRelType();
      try {
        List<KeyValue> curVals = new ArrayList<KeyValue>();
        boolean hasMore = false;
        byte[] preRow = null;
        int count = 0;
        do {
          curVals.clear();
          hasMore = scanner.next(curVals);
          for (KeyValue kv : curVals) {
            byte[] tailNodeId = getRelationTailNodeId(kv.getRow());
            if (preRow == null) preRow = tailNodeId;
            else if (Bytes.compareTo(preRow, tailNodeId) != 0) {
              if (count != 0) kvs.add(new KeyValue(preRow, familiy, Bytes.toBytes(count)));
              preRow = tailNodeId;
              count = 0;
            }
            byte[] row = kv.getRow();
            if (spec.getTimeRange().include(getTimestamp(row))
                && (reqRelType == null || (reqRelType != null && reqRelType
                    .contains(getRelationType(row))))) {
              List<byte[]> wdeIds = new SortedWdeRefSet(kv.getValue()).getWdeIdList();
              for (byte[] wdeId : wdeIds) {
                if (wdeIdSet == null || wdeIdSet.size() == 0) count++;
                else if (wdeIdSet.contains(new ByteArray(wdeId))) count++;
              }
            }
          }
        } while (hasMore);
        if (count != 0) kvs.add(new KeyValue(preRow, familiy, Bytes.toBytes(count)));
      } finally {
        scanner.close();
      }
      return new Result(kvs);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.info(e.getMessage());
      throw new IOException(e);
    }

  }

  @Override
  public MapWritable queryRelGraphRelationWdeRefsHTableDetail(RelQuerySpecWritable specWritable)
      throws IOException {// test may cost too many cache in server (use batch)
    try {
      RelQuerySpec spec = specWritable.getRelQuerySpec();
      Set<ByteArray> wdeIdSet = spec.getContainedWdeIds();// 有可能是null
      Node centerNode = spec.getCenterNode();
      byte[] familiy = RelationWdeRefsHTable.FAMILY;
      Scan scan = new Scan();
      scan.setBatch(Integer.MAX_VALUE);
      byte[] scanPrefix = Bytes.add(centerNode.getId(), spec.getRequiredAttribute().getByteFrom());
      Filter filter = new PrefixFilter(scanPrefix);
      scan.setFilter(filter).setCaching(1000);// TOD
      scan.setStartRow(scanPrefix);// for performance
      InternalScanner scanner = null;
      RegionCoprocessorEnvironment environment1 = (RegionCoprocessorEnvironment) getEnvironment();
      scanner = environment1.getRegion().getScanner(scan);
      MapWritable result  = new MapWritable();
      List<Pair<byte[],Map<RelationType,Integer>>> resultTmp = new ArrayList<Pair<byte[],Map<RelationType,Integer>>>();
      HashMap<RelationType, Integer> v = new HashMap<RelationType, Integer>();
      Set<RelationType> reqRelType = spec.getRequiredRelType();
      try {
        List<KeyValue> curVals = new ArrayList<KeyValue>();
        boolean hasMore = false;
        byte[] preRow = null;
        do {
          curVals.clear();
          hasMore = scanner.next(curVals);
          for (KeyValue kv : curVals) {
            byte[] tailNodeId = getRelationTailNodeId(kv.getRow());
            if (preRow == null) preRow = tailNodeId;
            else if (Bytes.compareTo(preRow, tailNodeId) != 0) {
              if (v.size() != 0) resultTmp.add(new Pair<byte[],Map<RelationType,Integer>>(preRow, v));
              preRow = tailNodeId;
             v.clear();
            }
            byte[] row = kv.getRow();
            RelationType relType = getRelationType(row);
            if (spec.getTimeRange().include(getTimestamp(row))
                && (reqRelType == null || (reqRelType != null && reqRelType
                    .contains(relType)))) {
              List<byte[]> wdeIds = new SortedWdeRefSet(kv.getValue()).getWdeIdList();
              for (byte[] wdeId : wdeIds) {
                if (wdeIdSet == null || wdeIdSet.size() == 0 || wdeIdSet.contains(new ByteArray(wdeId))) {
                  if(v.containsKey(relType))
                    v.put(relType, v.get(relType)+1);
                  else 
                    v.put(relType, 1);
                }
              }
            }
          }
        } while (hasMore);
        if (v.size()!= 0) resultTmp.add(new Pair<byte[],Map<RelationType,Integer>>(preRow, v));
      } finally {
        scanner.close();
      }
      for(Pair<byte[],Map<RelationType,Integer>> edge : resultTmp){
          MapWritable relations = new MapWritable();
          for(Entry<RelationType,Integer> relation: edge.getSecond().entrySet()){
            relations.put(new BytesWritable(relation.getKey().getBytesForm()), new IntWritable(relation.getValue()));
          }
          result.put(new BytesWritable(edge.getFirst()),relations);
      }
      return result;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.info(e.getMessage());
      throw new IOException(e);
    }

  }
  
  @Override
  public Result queryResultInEdgeRelWeightDetailHTable(RelQuerySpecWritable specWritable)
      throws Exception {
    // （region）由于是在RegionServer上进行扫描，盐值的大小要大于RegionServer的数量，因此并发scan还是有效果的。
    final RelQuerySpec spec = specWritable.getRelQuerySpec();
    final byte[] family = Bytes.toBytes('i');
    final Set<RelationType> reqRelType = spec.getRequiredRelType();
    final HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    final Node centerNode = spec.getCenterNode();
    final List<Pair<byte[], Integer>> mergeList = new ArrayList<Pair<byte[], Integer>>();
    // First, build Scans we need in each table partition.
    List<Scan> scanList = buildPrefixScansForAllPartitions(centerNode.getId(),
        spec.getRequiredAttribute());
    final List<KeyValue> kvs = new ArrayList<KeyValue>();
    ParallelTask<List<Pair<byte[], Integer>>> pTask = new ParallelTask<List<Pair<byte[], Integer>>>(
        exec) {
      @Override
      public void processResult(List<Pair<byte[], Integer>> result) {
        mergeList.addAll(result);
      }
    };
    for (final Scan scan : scanList) {
      // Parallel Scan in each partition of each table
      pTask.submitTasks(new Callable<List<Pair<byte[], Integer>>>() {
        @SuppressWarnings("unused")
        @Override
        public List<Pair<byte[], Integer>> call() throws IOException {
          List<Pair<byte[], Integer>> kvList = new ArrayList<Pair<byte[], Integer>>();
          InternalScanner scanner = null;
          try {
            scanner = region.getScanner(scan);
            List<KeyValue> curVals = new ArrayList<KeyValue>();
            boolean hasMore = false;
            byte[] preRow = null;
            int count = 0;
            do {
              curVals.clear();
              hasMore = scanner.next(curVals);// curVals are columns in a single row,so limit is not
                                              // suitable
              for (KeyValue kv : curVals) {
                byte[] tailNodeId = getEdgeTailNodeId(kv.getRow());
                if (preRow == null) preRow = tailNodeId;
                int flag = Bytes.compareTo(preRow, tailNodeId);
                if (flag != 0) { // pre!=next
                  if (count != 0) // count!=0 add preRow to KvList
                  kvList.add(new Pair<byte[], Integer>(preRow, count));
                  preRow = tailNodeId;
                  count = 0;
                }
                byte[] row = kv.getRow();
                RelationType relType = RelationType.getType(Bytes.toString(kv.getQualifier()));
                if (spec.getTimeRange().include(getTimestamp(row))
                    && (reqRelType == null || (reqRelType != null && reqRelType.contains(relType)))) {
                  count += Bytes.toLong(kv.getValue());
                }
              }
            } while (hasMore);
            if (count != 0) kvList.add(new Pair<byte[], Integer>(preRow, count));
          } finally {
            scanner.close();
          }
          return kvList;
        }
      });
    }
    pTask.gatherResults();// gather results
    Collections.sort(mergeList, new Comparator<Pair<byte[], Integer>>() {
      @Override
      public int compare(Pair<byte[], Integer> e1, Pair<byte[], Integer> e2) {
        return Bytes.compareTo(e1.getFirst(), e2.getFirst());// in ascending order.
      }
    });
    byte[] preEdge = null;
    int count = 0;
    for (Pair<byte[], Integer> edge : mergeList) {
      if (preEdge == null) {
        preEdge = edge.getFirst();
        count = edge.getSecond();
        continue;
      }
      if (Bytes.compareTo(edge.getFirst(), preEdge) == 0) {
        count += edge.getSecond();
      } else {
        kvs.add(new KeyValue(preEdge, family, Bytes.toBytes(count)));
        preEdge = edge.getFirst();
        count = edge.getSecond();
      }
    }

    if (preEdge != null) kvs.add(new KeyValue(preEdge, family, Bytes.toBytes(count)));
    return new Result(kvs);
  }

  @Override
  public MapWritable queryResultInEdgeRelWeightDetailHTableDetail(RelQuerySpecWritable specWritable)
      throws Exception {
    // （region）由于是在RegionServer上进行扫描，盐值的大小要大于RegionServer的数量，因此并发scan还是有效果的。
    final RelQuerySpec spec = specWritable.getRelQuerySpec();
    final byte[] family = Bytes.toBytes('i');
    final Set<RelationType> reqRelType = spec.getRequiredRelType();
    final HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    final Node centerNode = spec.getCenterNode();
    final MapWritable qresult = new MapWritable();
    // First, build Scans we need in each table partition.
    List<Scan> scanList = buildPrefixScansForAllPartitions(centerNode.getId(),
        spec.getRequiredAttribute());
    ParallelTask<List<Pair<byte[], Map<RelationType, Integer>>>> pTask = new ParallelTask<List<Pair<byte[], Map<RelationType, Integer>>>>(
        exec) {
      @Override
      public void processResult(List<Pair<byte[], Map<RelationType, Integer>>> result) {
        for (Pair<byte[], Map<RelationType, Integer>> edge : result) {
          BytesWritable nodeId = new BytesWritable(edge.getFirst());
          MapWritable v = (MapWritable) qresult.get(nodeId);
          if (v == null) {
            v = new MapWritable();
            qresult.put(nodeId, v);
          }
          for (Map.Entry<RelationType, Integer> e : edge.getSecond().entrySet()) {
            BytesWritable relType = new BytesWritable(e.getKey().getBytesForm());
            IntWritable count = (IntWritable) v.get(relType);
            if (count == null) v.put(relType, new IntWritable(e.getValue()));
            else count.set(count.get() + e.getValue());
          }
        }
      }
    };
    for (final Scan scan : scanList) {
      // Parallel Scan in each partition of each table
      pTask.submitTasks(new Callable<List<Pair<byte[], Map<RelationType, Integer>>>>() {
        @SuppressWarnings("unused")
        @Override
        public List<Pair<byte[], Map<RelationType, Integer>>> call() throws IOException {
          List<Pair<byte[], Map<RelationType, Integer>>> kvList = new ArrayList<Pair<byte[], Map<RelationType, Integer>>>();
          InternalScanner scanner = null;
          try {
            scanner = region.getScanner(scan);
            List<KeyValue> curVals = new ArrayList<KeyValue>();
            boolean hasMore = false;
            byte[] preRow = null;
            HashMap<RelationType, Integer> v = new HashMap<RelationType, Integer>();
            do {
              curVals.clear();
              hasMore = scanner.next(curVals);// curVals are columns in a single row,so limit is not
                                              // suitable
              for (KeyValue kv : curVals) {
                byte[] tailNodeId = getEdgeTailNodeId(kv.getRow());
                if (preRow == null) preRow = tailNodeId;
                int flag = Bytes.compareTo(preRow, tailNodeId);
                if (flag != 0) { // pre!=next
                  if (v.size() != 0) // edge.size()!=0 add preRow to KvList
                  kvList.add(new Pair<byte[], Map<RelationType, Integer>>(preRow, v));
                  preRow = tailNodeId;
                  v.clear();
                }
                byte[] row = kv.getRow();
                RelationType relType = RelationType.getType(Bytes.toString(kv.getQualifier()));
                if (spec.getTimeRange().include(getTimestamp(row))
                    && (reqRelType == null || (reqRelType != null && reqRelType.contains(relType)))) {
                  if (!v.containsKey(relType)) v.put(relType, 1);
                  else v.put(relType, v.get(relType) + 1);
                }
              }
            } while (hasMore);
            if (v.size() != 0) kvList.add(new Pair<byte[], Map<RelationType, Integer>>(preRow, v));
          } finally {
            scanner.close();
          }
          return kvList;
        }
      });
    }
    pTask.gatherResults();// gather results

    return qresult;
  }

  public MapWritable getRelationType() throws IOException {
    final HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    HashMap<String, byte[]> relationTypeMap = new HashMap<String, byte[]>();
    Scan scan = new Scan();
    scan.addFamily("i".getBytes());
    InternalScanner scanner = null;
    try {
      scanner = region.getScanner(scan);
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean hasMore = false;
      do {
        curVals.clear();
        hasMore = scanner.next(curVals);
        for (KeyValue kv : curVals) {
          String relationType = Bytes.toString(kv.getQualifier());
          if (!relationTypeMap.containsKey(relationType)) relationTypeMap.put(relationType,
              Bytes.tail(kv.getRow(), kv.getRow().length - 1));
        }
      } while (hasMore);
    } finally {
      scanner.close();
    }
    MapWritable result = new MapWritable();
    for (Map.Entry<String, byte[]> e : relationTypeMap.entrySet())
      result.put(new Text(e.getKey()), new BytesWritable(e.getValue()));
    return result;
  }

  public Result queryResultInEdgeRelWeightDetailHTableTest(RelQuerySpecWritable specWritable)
      throws Exception {
    // （region）由于是在RegionServer上进行扫描，盐值的大小要大于RegionServer的数量，因此并发scan还是有效果的。
    final RelQuerySpec spec = specWritable.getRelQuerySpec();
    final Set<RelationType> reqRelType = spec.getRequiredRelType();
    final HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    final Node centerNode = spec.getCenterNode();
    // First, build Scans we need in each table partition.
    List<Scan> scanList = buildPrefixScansForAllPartitions(centerNode.getId(),
        spec.getRequiredAttribute());
    final List<KeyValue> kvs = new ArrayList<KeyValue>();
    ParallelTask<List<KeyValue>> pTask = new ParallelTask<List<KeyValue>>(exec) {
      @Override
      public void processResult(List<KeyValue> result) {
        for (KeyValue kv : result)
          kvs.add(kv);
      }
    };
    for (final Scan scan : scanList) {
      // Parallel Scan in each partition of each table
      pTask.submitTasks(new Callable<List<KeyValue>>() {
        @Override
        public List<KeyValue> call() throws IOException {
          List<KeyValue> kvList = new ArrayList<KeyValue>();
          InternalScanner scanner = null;
          try {
            scanner = region.getScanner(scan);
            List<KeyValue> curVals = new ArrayList<KeyValue>();
            boolean hasMore = false;
            do {
              curVals.clear();
              hasMore = scanner.next(curVals);
              for (KeyValue kv : curVals) {
                byte[] row = kv.getRow();
                if (spec.getTimeRange().include(getTimestamp(row))
                    && (reqRelType == null || (reqRelType != null && reqRelType.contains(kv
                        .getQualifier())))) {// 这步仅仅对ts作了判断，因此，待返回KV时还需对relType做判断。
                  kvList.add(kv);// TODO 对start 及end的判重问题。
                }
              }
            } while (hasMore);
          } finally {
            scanner.close();
          }
          return kvList;
        }
      });
    }
    pTask.gatherResults();// gather results
    return new Result(kvs);
  }

  private int getTimestamp(byte[] row) {
    int length = row.length;
    return Bytes.toInt(row, length - 4, 4);
  }

  private RelationType getRelationType(byte[] row) {
    byte[] relationType = Arrays.copyOfRange(row, Edge.EDGEID_SIZE, row.length-4);
    return RelationType.getType(Bytes.toString(relationType));
  }

  private byte[] getEdgeTailNodeId(byte[] row) {

    return Arrays.copyOfRange(row, SALT_PREFIX_BYTE_SZIE + Node.NODEID_SIZE, SALT_PREFIX_BYTE_SZIE
        + Edge.EDGEID_SIZE);

  }

  private byte[] getRelationTailNodeId(byte[] row) {
    return Arrays.copyOfRange(row, Node.NODEID_SIZE, Node.NODEID_SIZE * 2);
  }

  /**
   * @param scanPrefix
   *          no salt byte needed.specify specific node type
   */
  private List<Scan> buildPrefixScansForAllPartitions(byte[] scanPrefix, Attribute attribute) {
    List<Scan> scanList = new ArrayList<Scan>(SALT_PARTITION_COUNT);
    for (byte salt = 0; salt < SALT_PARTITION_COUNT; salt++) {
      scanList.add(buildPrefixScan(Bytes.add(new byte[] { salt }, scanPrefix,
          attribute.getByteFrom())));
    }
    return scanList;
  }

  private Scan buildPrefixScan(byte[] scanPrefix) {
    Filter filter = new PrefixFilter(scanPrefix);
    Scan scan = new Scan();
    scan.setFilter(filter).setCaching(1000);// TODO
    scan.setStartRow(scanPrefix);// for performance
    return scan;
  }

  private HashSet<ByteArray> getWdeIdSet(BytesWritable wdeIds) {
    int length = wdeIds.getLength() / WdeRef.WDEID_SIZE;
    HashSet<ByteArray> wdeIdSet = new HashSet<ByteArray>(length);
    for (int i = 0; i < length; i++) {
      wdeIdSet.add(new ByteArray(Arrays.copyOfRange(wdeIds.getBytes(), i * WdeRef.WDEID_SIZE,
          (i + 1) * WdeRef.WDEID_SIZE)));
    }
    return wdeIdSet;
  }

  private int getTsInWdeRef(WdeRef wdeRef) {
    return Bytes.toInt(Arrays.copyOfRange(wdeRef.getWdeId(), 2, 6));
  }

  private Pair<Boolean, SortedWdeRefSet> updateWdeRefs(SortedWdeRefSet old, int start, int end) {
    boolean flag = false;
    List<WdeRef> wdeRefs = old.getList();
    List<WdeRef> newWdeRefs = new ArrayList<WdeRef>(wdeRefs.size());
    for (WdeRef wdeRef : wdeRefs) {
      int ts = getTsInWdeRef(wdeRef);
      if (ts >= start && ts < end) flag = true;
      else newWdeRefs.add(wdeRef);
    }
    return new Pair<Boolean, SortedWdeRefSet>(flag, new SortedWdeRefSet(newWdeRefs));
  }

  /**
   * @param start
   *          ts tart in seconds
   * @param end
   *          ts end in seconds
   * @return the number of deletes
   * @throws IOException
   */
  // TODO how to Guarantee the Security?
  public long cleanRelationTypeHTableByTS(byte[] start, byte[] end) throws IOException {
    int startInSecond = Bytes.toInt(Bytes.head(start, 4));
    int endInSecond = Bytes.toInt(Bytes.head(end, 4));
    if (endInSecond <= startInSecond) return 0;
    // LOG.info("start to delete RelationTypeHTableByTS, the start = "+ start+"s end = "+ end+"s.");
    HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    Scan scan = new Scan();
    // scan.setTimeRange(startInSecond, startInSecond);
    scan.addFamily(RelationWdeRefsHTable.FAMILY);
    InternalScanner scanner = null;
    long st = System.currentTimeMillis();
    long count = 0;
    long total = 0;

    LOG.info("start to delete RelationTypeHTableByTS in region: +" + region.getRegionNameAsString()
        + ". the start = " + startInSecond + "s end = " + endInSecond + "s.");
    BlockingQueue<Delete> rows = new LinkedBlockingQueue<Delete>(BATCH_SIZE * 2);
    CountDownLatch latch = new CountDownLatch(1);
    CleanRelationWdeRefTableHandler handler = new CleanRelationWdeRefTableHandler(region, latch,
        rows);
    Thread handlerThread = new Thread(handler);
    handlerThread.start();
    try {
      scanner = region.getScanner(scan);
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean hasMore = false;
      do {
        curVals.clear();
        hasMore = scanner.next(curVals);
        total++;
        if (curVals != null && curVals.size() > 0) {
          int ts = getTimestamp(curVals.get(0).getRow());
          if (startInSecond <= ts && ts < endInSecond) {
            Delete delete = new Delete(curVals.get(0).getRow());
            rows.put(delete);
            count++;
          }
        }
      } while (hasMore);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      try {
        rows.put(CleanRelationWdeRefTableHandler.POISON_OBJECT);
        latch.await();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      scanner.close();
    }
    LOG.info("finish  delete RelationTypeHTableByTS in region: +" + region.getRegionNameAsString()
        + ". the start = " + startInSecond + "s end = " + endInSecond + "s in "
        + (System.currentTimeMillis() - st) + "ms, total rows :" + total + ", changed rows :"
        + count);
    return count;
  }

  public long cleanEdgeRelWeightDetailHTableByTs1(byte[] start, byte[] end) throws IOException {
    int startInSecond = Bytes.toInt(Bytes.head(start, 4));
    int endInSecond = Bytes.toInt(Bytes.head(end, 4));
    if (endInSecond <= startInSecond) return 0;
    long st = System.currentTimeMillis();
    HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    LOG.info("start to delete EdgeRelWeightDetailHTableByTs in region: +"
        + region.getRegionNameAsString() + ". the start = " + startInSecond + "s end = "
        + endInSecond + "s.");
    Configuration conf = HBaseConfiguration.create();
    // edgeIdTable to
    HTable edgeIdTable = new HTable(conf, region
        .getTableDesc()
        .getNameAsString()
        .replaceFirst(GdbHTableType.EDGE_REL_WEIGHT_DETAIL.getContentTag(),
            GdbHTableType.EDGE_ID.getContentTag()));
    HTable edgeWeightSumTable = new HTable(conf, region
        .getTableDesc()
        .getNameAsString()
        .replaceFirst(GdbHTableType.EDGE_REL_WEIGHT_DETAIL.getContentTag(),
            GdbHTableType.EDGE_REL_WEIGHT_SUM.getContentTag()));
    edgeIdTable.setAutoFlush(true);
    edgeWeightSumTable.setAutoFlush(true);
    BlockingQueue<DelEdge> delEdges = new LinkedBlockingQueue<DelEdge>(BATCH_SIZE * 2);
    CountDownLatch latch = new CountDownLatch(1);
    CleanEdgeDetailHandler handler = new CleanEdgeDetailHandler(edgeIdTable, edgeWeightSumTable,
        region, latch, delEdges);
    Thread handlerThread = new Thread(handler);
    handlerThread.start();
    long count = 0;
    long total = 0;
    // List<DelEdge> delEdges =new ArrayList<DelEdge>(BATCH_SIZE);
    Scan scan = new Scan();
    scan.setCaching(10000);
    scan.addFamily(EdgeRelWeightDetailHTable.FAMILY);
    InternalScanner scanner = null;
    try {
      scanner = region.getScanner(scan);
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean hasMore = false;
      DelEdge pre = null;
      do {
        total++;
        curVals.clear();
        hasMore = scanner.next(curVals);
        if (curVals.size() == 0) break;
        int ts = getTimestamp(curVals.get(0).getRow());
        if (startInSecond <= ts && ts < endInSecond) {
          count++;
          if (pre == null) pre = new DelEdge(curVals);
          else if (!pre.add(curVals)) {
            delEdges.put(pre);
            pre = new DelEdge(curVals);
          }
        }
      } while (hasMore);
      if (pre != null) delEdges.put(pre);

    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      try {
        delEdges.put(DelEdge.POISON_OBJECT);// scan end put in the POISON_OBJECT so the thread can
                                            // jump out the loop.
        latch.await();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      scanner.close();
      edgeIdTable.close();
      edgeWeightSumTable.close();
    }
    LOG.info("finish  delete EdgeRelWeightDetailHTableByTs in region: +"
        + region.getRegionNameAsString() + ". the start = " + startInSecond + "s end = "
        + endInSecond + "s in " + (System.currentTimeMillis() - st) + "ms, total rows :" + total
        + ", changed rows :" + count);
    return count;

  }

  public long cleanEdgeRelWeightDetailHTableByTs(byte[] start, byte[] end) throws IOException {
    int startInSecond = Bytes.toInt(Bytes.head(start, 4));
    int endInSecond = Bytes.toInt(Bytes.head(end, 4));
    if (endInSecond <= startInSecond) return 0;
    long st = System.currentTimeMillis();
    HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    LOG.info("start to delete EdgeRelWeightDetailHTableByTs in region: +"
        + region.getRegionNameAsString() + ". the start = " + startInSecond + "s end = "
        + endInSecond + "s.");
    Configuration conf = HBaseConfiguration.create();
    // edgeIdTable to
    HTable edgeIdTable = new HTable(conf, region
        .getTableDesc()
        .getNameAsString()
        .replaceFirst(GdbHTableType.EDGE_REL_WEIGHT_DETAIL.getContentTag(),
            GdbHTableType.EDGE_ID.getContentTag()));
    HTable edgeWeightSumTable = new HTable(conf, region
        .getTableDesc()
        .getNameAsString()
        .replaceFirst(GdbHTableType.EDGE_REL_WEIGHT_DETAIL.getContentTag(),
            GdbHTableType.EDGE_REL_WEIGHT_SUM.getContentTag()));
    edgeIdTable.setAutoFlush(true);
    edgeWeightSumTable.setAutoFlush(true);
    long count = 0;
    long total = 0;
    List<DelEdge> delEdges = new ArrayList<DelEdge>(BATCH_SIZE);
    List<Delete> deletes = new ArrayList<Delete>(BATCH_SIZE * 2);
    Scan scan = new Scan();
    scan.setCaching(10000);
    scan.addFamily(EdgeRelWeightDetailHTable.FAMILY);
    InternalScanner scanner = null;
    try {
      scanner = region.getScanner(scan);
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean hasMore = false;
      DelEdge pre = null;
      do {
        total++;
        curVals.clear();
        hasMore = scanner.next(curVals);
        int ts = getTimestamp(curVals.get(0).getRow());
        if (startInSecond <= ts && ts < endInSecond) {
          Delete delete = new Delete(curVals.get(0).getRow());
          deletes.add(delete);
          count++;
          if (pre == null) pre = new DelEdge(curVals);
          else if (!pre.add(curVals)) {
            delEdges.add(pre);
            if (delEdges.size() == BATCH_SIZE) {
              cleanEdgeIdAndEdgeSumTable(edgeIdTable, edgeWeightSumTable, delEdges);
              for (Delete delete1 : deletes)
                region.delete(delete1, false);// writeToWAL can be false?
              deletes.clear();
              delEdges.clear();
            }
            pre = new DelEdge(curVals);
          }
        }
      } while (hasMore);
      if (pre != null) delEdges.add(pre);
      if (delEdges.size() > 0) {
        cleanEdgeIdAndEdgeSumTable(edgeIdTable, edgeWeightSumTable, delEdges);
        delEdges.clear();
        for (Delete delete1 : deletes)
          region.delete(delete1, false);// writeToWAL can be false?
        deletes.clear();
      }
    } finally {
      scanner.close();
      edgeIdTable.close();
      edgeWeightSumTable.close();
    }
    LOG.info("finish  delete EdgeRelWeightDetailHTableByTs in region: +"
        + region.getRegionNameAsString() + ". the start = " + endInSecond + "s end = "
        + endInSecond + "s in " + (System.currentTimeMillis() - st) + "ms, total rows :" + total
        + ", changed rows :" + count);
    return count;
  }

  /**
   * steps:<br>
   * 1,batch get from edgeWeightSumTable for rows that has bean modified in edgeWeightDetailTable.<br>
   * 2, reGenDelEdge to generate the delete or change(increment) information for edgeIdTable and
   * edgeWeightSumTable.<br>
   * 3, construct the batch change list for edgeIdTable and edgeWeightSumTable.<br>
   * 4, In a certain order to change the table edgeIdTable -> edgeWeightSumTable<br>
   * 
   * TODO use producer/customer to improve performance!(BlockingQueue)
   * */
  private void cleanEdgeIdAndEdgeSumTable(HTable edgeIdTable, HTable edgeWeightSumTable,
      List<DelEdge> delEdges) {
    long st = System.currentTimeMillis();
    List<Delete> edgeIdDeltes = new ArrayList<Delete>(); //
    // List<Delete> edgeWeightSumDeletes=new ArrayList<Delete>();
    List<Row> edgeWeightSumChanges = new ArrayList<Row>(); // all in one region,good!
    List<Get> gets = new ArrayList<Get>(BATCH_SIZE);
    for (DelEdge delEdge : delEdges) {
      Get get = new Get(delEdge.getId());
      get.addFamily(EdgeRelWeightSumHTable.FAMILY);
      gets.add(get);
    }
    try {
      Result[] results = edgeWeightSumTable.get(gets);
      LOG.info("Bach get in edgeWeightSumTable in " + (System.currentTimeMillis() - st) + "ms");
      Iterator<DelEdge> delEdgeIter = delEdges.iterator();
      for (Result result : results) {
        DelEdge delEdge = delEdgeIter.next();
        if (result == null || result.isEmpty()) {
          delEdgeIter.remove();
        } else {
          delEdge.reGenDelEdgeFromWeightSum(result);
        }
      }

      for (DelEdge delEdge : delEdges) {
        if (delEdge.isDelete()) {
          Delete delete = new Delete(Bytes.tail(delEdge.getId(), Edge.EDGEID_SIZE));
          edgeIdDeltes.add(delete);
          Delete delete1 = new Delete(delEdge.getId());
          edgeWeightSumChanges.add(delete1);
        } else {
          for (Map.Entry<ByteArray, Long> e : delEdge.getRelationTypeWeight().entrySet()) {
            if (e.getValue() == 0) {
              Delete delete1 = new Delete(delEdge.getId());
              delete1.deleteColumns(EdgeRelWeightSumHTable.FAMILY, e.getKey().getBytes());
            } else if (e.getValue() < 0) {
              Increment increment = new Increment(delEdge.getId());
              increment.addColumn(EdgeRelWeightSumHTable.FAMILY, e.getKey().getBytes(),
                  e.getValue());
              edgeWeightSumChanges.add(increment);
            }
          }
        }
      }
      try {
        st = System.currentTimeMillis();
        Result[] results1 = new Result[edgeIdDeltes.size()]; // all result should be result.empty
        Result[] results2 = new Result[edgeWeightSumChanges.size()];
        edgeIdTable.batch(edgeIdDeltes, results1);// Just use the retries of HBase
        LOG.info("Bach delete in edgeIdTable in " + (System.currentTimeMillis() - st) + "ms");
        st = System.currentTimeMillis();
        edgeWeightSumTable.batch(edgeWeightSumChanges, results2);
        LOG.info("Bach changes in edgeWeightSumTable in " + (System.currentTimeMillis() - st)
            + "ms");

      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    } catch (IOException e) {
      // TODO what?just ignore;
      e.printStackTrace();
    }
  }

  private List<byte[]> getIds(BytesWritable ids) {
    byte[] idsByte = Bytes.head(ids.getBytes(), ids.getLength());
    // LOG.info("2 ids to judge: "+StringUtils.byteToHexString(idsByte));
    int len = idsByte.length / Node.NODEID_SIZE;
    List<byte[]> idList = new ArrayList<byte[]>(len);
    for (int i = 0; i < idsByte.length; i = i + Node.NODEID_SIZE) {
      idList.add(Arrays.copyOfRange(idsByte, i, i + Node.NODEID_SIZE));
    }
    return idList;
  }

  public BytesWritable getIdsToDeleteByEdgeIdHTable(BytesWritable ids) throws IOException {
    List<byte[]> idList = getIds(ids);
    byte[] result = new byte[idList.size()];// each byte corresponds one node id, if the byte ==
                                            // 0x00 then the id does not exist in this region!
    for (int i = 0; i < idList.size(); i++) {
      result[i] = 0x00;
    }
    HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    byte[] startRow = idList.get(0);
    byte[] stopRow = Bytes.add(idList.get(idList.size() - 1), new byte[] { (byte) 0xff });// inclusive
                                                                                          // the
                                                                                          // last
                                                                                          // row !
    Scan scan = new Scan();
    scan.setStartRow(startRow);
    scan.setStopRow(stopRow);
    scan.setCaching(10000);
    scan.addFamily(EdgeIdHTable.FAMILY);
    InternalScanner scanner = null;
    List<byte[]> existsIds = new ArrayList<byte[]>(idList.size());
    try {
      scanner = region.getScanner(scan);
      List<KeyValue> curVals = new ArrayList<KeyValue>();
      boolean hasMore = false;
      byte[] pre = null;
      do {
        curVals.clear();
        hasMore = scanner.next(curVals);
        for (KeyValue kv : curVals) {
          byte[] nodeId = Bytes.head(kv.getRow(), Node.NODEID_SIZE);
          if (pre == null) pre = nodeId;
          else if (!Bytes.equals(pre, nodeId)) {
            existsIds.add(pre);
            pre = nodeId;
          }
        }
      } while (hasMore);
      if (pre != null) existsIds.add(pre);
      if (existsIds.size() == 0) return new BytesWritable(result);
      Iterator<byte[]> existsIdsIter = existsIds.iterator();
      Iterator<byte[]> idListIter = idList.iterator();
      int index = 0;
      byte[] existsId = existsIdsIter.next();
      byte[] id = idListIter.next();
      int flag = 0;
      do {
        flag = Bytes.compareTo(existsId, id);
        if (flag == 0) {
          result[index] = 0x01;
          existsId = existsIdsIter.next();
          id = idListIter.next();
          index++;
        } else if (flag < 0) {// this should not happen
          LOG.warn("flag<0 ? this should not happen, find out why!!!!");
          existsId = existsIdsIter.next();
        } else {
          id = idListIter.next();
          index++;
        }
      } while (idListIter.hasNext() && existsIdsIter.hasNext());
      if (Bytes.equals(existsId, id)) result[index] = 0x01;
    } finally {
      scanner.close();

    }

    return new BytesWritable(result);
  }

}