package ict.ada.gdb.dao;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.common.model.PathGraph;
import ict.ada.common.model.Relation;
import ict.ada.common.model.RelationGraph;
import ict.ada.common.model.RelationType;
import ict.ada.common.model.WdeRef;
import ict.ada.common.util.ByteArray;
import ict.ada.common.util.Hex;
import ict.ada.common.util.MapTool;
import ict.ada.common.util.Pair;
import ict.ada.common.util.Timer;
import ict.ada.common.util.Triplet;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.PathQuerySpec;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.common.RelQuerySpecWritable;
import ict.ada.gdb.common.SortedWdeRefSet;
import ict.ada.gdb.common.TimeRange;
import ict.ada.gdb.coprocessor.HBaseEdgeDaoProtocol;
import ict.ada.gdb.schema.EdgeIdHTable;
import ict.ada.gdb.schema.EdgeMemTable;
import ict.ada.gdb.schema.EdgeRelWeightDetailHTable;
import ict.ada.gdb.schema.EdgeRelWeightSumHTable;
import ict.ada.gdb.schema.GdbHTableConstant;
import ict.ada.gdb.schema.RelationTypeHTable;
import ict.ada.gdb.schema.RelationWdeRefsHTable;
import ict.ada.gdb.util.ParallelTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

/**
 * Operations on Edges in HBase
 * 
 */
/**
 * 
 */
public class HBaseEdgeDAO {
  private static Log LOG = LogFactory.getLog(HBaseEdgeDAO.class);
  private final ExecutorService exec;
  private static ConcurrentHashMap<Channel, HashSet<RelationType>> relationTypeMap = new ConcurrentHashMap<Channel, HashSet<RelationType>>();
  private GdbHTablePool pool;

  public HBaseEdgeDAO(GdbHTablePool pool) {
    this.pool = pool;
    exec = new ThreadPoolExecutor(40, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>());// core pool
    // size = 40
  }

  /**
   * Add an Edge to several GDB tables.
   * <p>
   * It's supposed that:<br>
   * 1)Edge's end Nodes have been added.<br>
   * 2)Each Node has its id.<br>
   * 
   * @param edge
   * @throws GdbException
   */
  public void addDirectedEdge(Edge edge) throws GdbException {
    if (edge == null) throw new NullPointerException("null Edge");
    // When storing an Edge, we need to store it into 4 tables.
    // Store into different HTable according to Edge head and tail's
    // NodeType
    try {
      // First, save Edge id
      saveEdgeId(edge);

      // Then, save each Relation
      if (edge.getRelations().size() != 0) {
        for (Relation rel : edge.getRelations()) {
          // addRelationType(edge.getHeadNodeType(),rel);
          addRelation(rel);
        }
      }
      // Done!
    } catch (IOException ioe) {
      throw new GdbException(ioe);
    }
  }
  public void addDirectedEdgeWithComputation(Edge edge) throws GdbException {
	    if (edge == null) throw new NullPointerException("null Edge");
	    try {
	      if (edge.getRelations().size() != 0) {
	        for (Relation rel : edge.getRelations()) {
	        	addRealtionWithComputation(rel);
	        }
	      }
	    } catch (IOException ioe) {
	      throw new GdbException(ioe);
	    }
	  }
  private static final byte[] EMPTY_VALUE = new byte[0];
  private static final byte[] EMPTY_QUALIFIER = new byte[0];

  private void addRelationType(NodeType nodeType, Relation relation) throws IOException {
    HashSet<RelationType> relationTypes = relationTypeMap.get(nodeType.getChannel());
    if (relationTypes != null && relationTypes.contains(relation.getType())) return;
    else if (relationTypes == null) {
      relationTypes = new HashSet<RelationType>();
      relationTypes.add(relation.getType());
      relationTypeMap.put(nodeType.getChannel(), relationTypes);
    } else relationTypes.add(relation.getType());
    // 为了不影响入库速度,在此不再入关系类型表.
    /*
     * HTableInterface relationTypeTable = pool.getRelationTypeTable(); try { // It's supposed that
     * edge id is available as we have inserted the // end nodes first. byte[] row
     * =Bytes.add(nodeType.getChannel().getByteFrom(), relation.getType().getBytesForm()); if
     * (!relationTypeTable.exists(new Get(row))) { Put put = new Put(row);// edge id as row key
     * put.add(RelationTypeHTable.FAMILY, RelationTypeHTable.QUALIFIER,
     * relation.getParentEdge().getId()); relationTypeTable.put(put); } } finally {
     * closeHTable(relationTypeTable); }
     */
  }

  private void saveEdgeId(Edge edge) throws IOException {
    HTableInterface edgeIdTable = pool.getEdgeIdTable(edge.getHeadNodeType());
    try {
      // It's supposed that edge id is available as we have inserted the
      // end nodes first.
      if (!edgeIdTable.exists(new Get(edge.getId()))) {
        Put put = new Put(edge.getId());// edge id as row key
        put.add(EdgeIdHTable.FAMILY, EMPTY_QUALIFIER, EMPTY_VALUE);
        edgeIdTable.put(put);
      }
    } finally {
      closeHTable(edgeIdTable);
    }
  }

  /**
   * delete node and its related edges .
   * 
   * @param node
   *          the node to delete
   * @throws Exception
   */
  public void deleteEdge(Node node) throws Exception {
    byte[] scanPrefix = node.getId();
    // get edgeIds to delete in edgeId table
    List<byte[]> edgeIds = scanForEdgeIds(scanPrefix, node.getType());
    // the edgeids to delete
    HTableInterface edgeIdTable = pool.getEdgeIdTable(node.getType());
    List<byte[]> edgesIdsToD = new ArrayList<byte[]>(edgeIds);
    for (byte[] edgeId : edgeIds) {
      // Flip Edge should delete
      byte[] creEdgeId = Bytes.add(Bytes.tail(edgeId, edgeId.length / 2), node.getId());
      boolean exists = edgeIdTable.exists(new Get(creEdgeId));
      if (exists) {
        edgesIdsToD.add(creEdgeId);
      }
    }
    // group the deletes in each table
    HashMap<HTableInterface, List<Delete>> deletesMap = new HashMap<HTableInterface, List<Delete>>();
    HTableInterface edgeRelWeightSumTable = pool.getEdgeRelWeightSumTable(node.getType());
    List<Delete> deletesEdgeId = new ArrayList<Delete>();// deletes object
    // in
    // edgeIdTable
    List<Delete> deletesRelWSum = new ArrayList<Delete>();// deletes object
    // in
    // RelWeightSumTable
    for (byte[] edgeId : edgesIdsToD) {
      deletesEdgeId.add(new Delete(edgeId));
      deletesRelWSum.add(new Delete(getSaltedRowKey(edgeId)));
    }
    deletesMap.put(edgeIdTable, deletesEdgeId);
    deletesMap.put(edgeRelWeightSumTable, deletesRelWSum);
    deletesMap.put(pool.getEdgeRelWeightDetailTable(node.getType()),
        getDeletesInEdgeRelWeightDetailTable(edgesIdsToD, node.getType()));
    deletesMap.put(pool.getRelationWdeRefsTable(node.getType()),
        getDeletesInRelationWdeRefsTable(edgesIdsToD, node.getType()));
    Map<HTableInterface, Boolean> flags = new HashMap<HTableInterface, Boolean>();
    for (Entry<HTableInterface, List<Delete>> e : deletesMap.entrySet()) {// delete
      // in
      // each
      // table;
      flags.put(e.getKey(), deleteEdgesInSingleTable(e.getKey(), e.getValue()));
    }
    // tableNames that is not successfully deleted;
    ArrayList<String> tableNames = new ArrayList<String>();
    for (Entry<HTableInterface, Boolean> e : flags.entrySet()) {
      if (!e.getValue()) tableNames.add(Bytes.toString(e.getKey().getTableName()));
    }
    if (tableNames.size() != 0) throw new Exception("delete in tables " + tableNames.toString()
        + " fails after retry ten times.");
  }

  /**
   * get deletes object in EdgeRelWeightDetailTable for total edges
   */
  private List<Delete> getDeletesInEdgeRelWeightDetailTable(final List<byte[]> edgeIds,
      NodeType nodeType) throws GdbException {
    final List<Delete> results = new ArrayList<Delete>();
    final int batchsize = 100; // each thread to scan the number of edges;
    ParallelTask<List<Delete>> pTask = new ParallelTask<List<Delete>>(exec) {
      @Override
      public void processResult(List<Delete> result) {
        results.addAll(result);
      }
    };
    final HTableInterface edgeRelWDetailTable = pool.getEdgeRelWeightDetailTable(nodeType);
    for (int i = 0; i < edgeIds.size(); i += batchsize) {
      final int current = i;
      pTask.submitTasks(new Callable<List<Delete>>() {
        @Override
        public List<Delete> call() throws Exception {
          List<Delete> deletes = new ArrayList<Delete>();
          for (int j = current; j < batchsize && j < edgeIds.size(); j++) {
            List<Scan> scanlist = buildPrefixScansForAllPartitions(edgeIds.get(j), true);
            for (Scan scan : scanlist) {
              ResultScanner rs = edgeRelWDetailTable.getScanner(new Scan(scan));
              try {
                for (Result result : rs) {
                  deletes.add(new Delete(result.getRow()));
                }
              } finally {
                rs.close();
              }
            }
          }
          return deletes;
        }

      });
    }
    try {
      pTask.gatherResults();// gather results
    } catch (Exception e) {
      throw new GdbException(e);
    }
    return results;
  }

  /**
   * get deletes object in RelationWdeRefsTable for total edges
   */
  private List<Delete> getDeletesInRelationWdeRefsTable(final List<byte[]> edgeIds,
      NodeType nodeType) throws GdbException {
    final List<Delete> results = new ArrayList<Delete>();
    final int batchsize = 100;
    ParallelTask<List<Delete>> pTask = new ParallelTask<List<Delete>>(exec) {
      @Override
      public void processResult(List<Delete> result) {
        results.addAll(result);
      }
    };
    final HTableInterface edgeRelWDetailTable = pool.getRelationWdeRefsTable(nodeType);
    for (int i = 0; i < edgeIds.size(); i += batchsize) {
      final int current = i;
      pTask.submitTasks(new Callable<List<Delete>>() {
        @Override
        public List<Delete> call() throws Exception {
          List<Delete> deletes = new ArrayList<Delete>();
          for (int j = current; j < batchsize && j < edgeIds.size(); j++) {
            Scan scan = new Scan();
            FirstKeyOnlyFilter keyonlyfilter = new FirstKeyOnlyFilter();
            PrefixFilter prefilter = new PrefixFilter(edgeIds.get(j));
            FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            fl.addFilter(prefilter);
            fl.addFilter(keyonlyfilter);
            scan.setStartRow(edgeIds.get(j));
            scan.setFilter(fl);
            ResultScanner rs = edgeRelWDetailTable.getScanner(new Scan(scan));
            try {
              for (Result result : rs) {
                deletes.add(new Delete(result.getRow()));
              }
            } finally {
              rs.close();
            }
          }
          return deletes;
        }

      });
    }

    try {
      pTask.gatherResults();// gather results
    } catch (Exception e) {
      throw new GdbException(e);
    }
    return results;
  }

  private boolean deleteEdgesInSingleTable(HTableInterface table, List<Delete> deletes)
      throws IOException {
    int i = 0;
    boolean flag = false;
    try {
      for (; i < 10; i++) {
        // 如果deletes数量大的化，会有超时危险，应当把deletes分批！测试过程中改进
        table.delete(deletes);
        if (deletes.size() == 0) {
          flag = true;
          break;
        }
      }
    } finally {
      closeHTable(table);
    }
    return flag;
  }

  private void deleteEdgeId(Edge edge) throws IOException {
    HTableInterface edgeIdTable = pool.getEdgeIdTable(edge.getHeadNodeType());
    try {
      if (!edgeIdTable.exists(new Get(edge.getId()))) {
        Delete delete = new Delete(edge.getId());// edge id as row key
        edgeIdTable.delete(delete); // just delete right away? or just
        // return a object of Delete
      }
    } finally {
      closeHTable(edgeIdTable);
    }
  }

  private List<byte[]> getDeleteEdegeId(Node node) throws Exception {
    byte[] scanPrefix = node.getId();
    List<byte[]> edgeIds = scanForEdgeIds(scanPrefix, node.getType());
    List<byte[]> edgeIdsCre = new ArrayList<byte[]>();
    for (byte[] edgeId : edgeIds) {
      edgeIdsCre.add(Bytes.add(Bytes.tail(edgeId, edgeId.length / 2), node.getId()));
    }
    edgeIds.addAll(edgeIdsCre);
    return edgeIds;
  }

  private void addRelation(Relation rel) throws IOException {
    // add weight
    addRelationWeight(rel);
    // add wdeRefs
    if (rel.getWdeRefs().size() != 0) {// Some relations do not contain
      // WdeRefs
      addRelationWdeRefs(rel);
    }
  }
  private void addRealtionWithComputation(Relation rel) throws IOException{
		Edge relParent = rel.getParentEdge();
	    NodeType edgeHeadType = rel.getParentEdgeHeadNodeType();
	    byte[] relTypeCol = rel.getType().getBytesForm();
	    // Increment weight sum
	    HTableInterface edgeMemTable = pool.getEdgeMemTable(edgeHeadType);
	    try {
	      int relWeight = rel.getWeight() <= 0 ? 1 : rel.getWeight();
	      Put put = new Put(relParent.getId());
	      put.add(EdgeMemTable.FAMILY,relTypeCol,Bytes.toBytes(relWeight));
	      edgeMemTable.put(put);
	    } finally {
	      closeHTable(edgeMemTable);
	    }
	}
  private static final int SALT_PARTITION_COUNT = 32;// TODO determine table
  // scalability
  private static final int SALT_PREFIX_BYTE_SZIE = 1;// one byte salt prefix

  /**
   * Hash function to distribute data across HBase regions. <br>
   * Once determined, should never be changed.Otherwise, a total data rehash is needed.
   * 
   * @param bytes
   * @return
   */
  private static byte saltHash(byte[] bytes) {
    // This hash function is from Arrays.hashCode(byte[])
    if (bytes == null) return 0;
    int result = 1;
    for (byte element : bytes) {
      result = 31 * result + element;
    }
    return (byte) ((result % SALT_PARTITION_COUNT + SALT_PARTITION_COUNT) % SALT_PARTITION_COUNT);
  }

  /**
   * return edges exist in the EdgeIdHTable (GDB) from the given edges.
   * 
   * @param edges
   * @return
   * @throws GdbException
   */
  public Pair<List<Edge>, List<Edge>> getExistEdges(List<Edge> edges) throws GdbException {
    Pair<List<Edge>, List<Edge>> edgespair = new Pair<List<Edge>, List<Edge>>();
    List<Edge> notExistEdges = new ArrayList<Edge>();
    List<Edge> existEdges = new ArrayList<Edge>();
    Map<String, List<Edge>> edgesinTables = new HashMap<String, List<Edge>>();
    for (Edge edge : edges) {
      String tableName = EdgeIdHTable.getName(edge.getHeadNodeType());
      if (!edgesinTables.containsKey(tableName)) {
        List<Edge> edgesinTable = new ArrayList<Edge>();
        edgesinTable.add(edge);
        edgesinTables.put(tableName, edgesinTable);
      } else edgesinTables.get(tableName).add(edge);
    }
    try {
      for (Entry<String, List<Edge>> e : edgesinTables.entrySet()) {
        Pair<List<Edge>, List<Edge>> edgespairTable = getExistEdgesInSameTable(e.getValue());
        existEdges.addAll(edgespairTable.getFirst());
        notExistEdges.addAll(edgespairTable.getSecond());
      }
      edgespair.setFirst(existEdges);
      edgespair.setSecond(notExistEdges);
    } catch (IOException e1) {
      throw new GdbException(e1);
    } catch (InterruptedException e1) {
      throw new GdbException(e1);
    }
    return edgespair;
  }

  /**
   * edges must in the same table.
   * 
   * @param edges
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  private Pair<List<Edge>, List<Edge>> getExistEdgesInSameTable(List<Edge> edges)
      throws IOException, InterruptedException {
    Pair<List<Edge>, List<Edge>> edgespair = new Pair<List<Edge>, List<Edge>>();
    List<Edge> existEdges = new ArrayList<Edge>();
    List<Edge> notExistEdges = new ArrayList<Edge>();
    HTableInterface edgeIdTable = pool.getEdgeIdTable(edges.get(0).getHeadNodeType());
    List<Get> batch = new ArrayList<Get>();
    for (Edge edge : edges) {
      Get get = new Get(edge.getId());
      get.addFamily(EdgeIdHTable.FAMILY);
      batch.add(get);
    }
    try {
      Object[] results = new Object[batch.size()];
      edgeIdTable.batch(batch, results);
      for (int i = 0; i < results.length; i++) {
        Result re = (Result) results[i];
        if (re.isEmpty()) notExistEdges.add(edges.get(i));
        else existEdges.add(edges.get(i));
      }
      edgespair.setFirst(existEdges);
      edgespair.setSecond(notExistEdges);
    } finally {
      closeHTable(edgeIdTable);
    }

    return edgespair;
  }

  /**
   * Update relation weight in two HTables.
   */
  private void addRelationWeight(Relation rel) throws IOException {
    Edge relParent = rel.getParentEdge();
    NodeType edgeHeadType = rel.getParentEdgeHeadNodeType();
    byte[] relTypeCol = rel.getType().getBytesForm();
    // Increment weight sum
    HTableInterface edgeWeightSumTable = pool.getEdgeRelWeightSumTable(edgeHeadType);
    try {
      int relWeight = rel.getWeight() <= 0 ? 1 : rel.getWeight();
      edgeWeightSumTable.incrementColumnValue(getSaltedRowKey(relParent.getId()),
          EdgeRelWeightSumHTable.FAMILY, relTypeCol, relWeight);
    } finally {
      closeHTable(edgeWeightSumTable);
    }
    // Increment weight in each time range
    List<WdeRef> refList = rel.getWdeRefs();
    if (refList != null) {// Some relations do not contain WdeRefs
      // pre-compute tsStart <->weight, so that for each tsStart we need
      // only one HBase
      // incrementColumnValue
      Map<Integer, Integer> tsStartToWeight = new HashMap<Integer, Integer>();
      for (WdeRef ref : refList) {
        int tsRangeStart = getTsRangeStart(ref.getTimestamp());
        Integer curWeight = tsStartToWeight.get(tsRangeStart);
        if (curWeight == null) {
          tsStartToWeight.put(tsRangeStart, 1);
        } else {
          tsStartToWeight.put(tsRangeStart, curWeight + 1);
        }
      }
      HTableInterface edgeWeightTable = pool.getEdgeRelWeightDetailTable(edgeHeadType);
      try {
        for (Entry<Integer, Integer> entry : tsStartToWeight.entrySet()) {
          byte[] rowkeyWithSalt = getSaltedRowKey(relParent.getId());
          byte[] rowkeyWithSaltandTs = Bytes.add(rowkeyWithSalt, Bytes.toBytes(entry.getKey()));
          // byte[] rowkeyWithTs = Bytes.add(relParent.getId(),
          // Bytes.toBytes(entry.getKey()));
          edgeWeightTable.incrementColumnValue(rowkeyWithSaltandTs,
              EdgeRelWeightDetailHTable.FAMILY, relTypeCol, entry.getValue());
        }
      } finally {
        closeHTable(edgeWeightTable);
      }
    }
  }

  /**
   * Get the salted row key with one leading salt byte.
   */
  public static byte[] getSaltedRowKey(byte[] rowKey) {
    byte[] result = new byte[rowKey.length + 1];
    result[0] = saltHash(rowKey);
    System.arraycopy(rowKey, 0, result, 1, rowKey.length);
    return result;
  }

  public static byte[] getEdgeIdFromSaltedRowKey(byte[] saltedRowkey) {
    return Arrays.copyOfRange(saltedRowkey, 1, 1 + Edge.EDGEID_SIZE);
  }

  public byte[] getEdgeIdFromSaltedRowKeyWithTs(byte[] saltedRowkey) {
    return Arrays.copyOfRange(saltedRowkey, SALT_PREFIX_BYTE_SZIE, saltedRowkey.length - 4);
  }

  private void addRelationWdeRefs(Relation rel) throws IOException {
    Edge relParent = rel.getParentEdge();
    HTableInterface relWdeRefsTable = pool.getRelationWdeRefsTable(rel.getParentEdgeHeadNodeType());
    try {
      List<WdeRef> refListToAdd = rel.getWdeRefs();
      Map<Integer, List<WdeRef>> groupedWdeRefsMap = groupWdeRefList(refListToAdd);
      for (Entry<Integer, List<WdeRef>> entry : groupedWdeRefsMap.entrySet()) {
        int tsRangeStart = entry.getKey();
        byte[] rowKey = Bytes.add(relParent.getId(), rel.getType().getBytesForm(),
            Bytes.toBytes(tsRangeStart));
        boolean success = false;
        while (!success) {// Try Get() and checkAndPut() until success
          // LOG.info(StringUtils.byteToHexString(rowKey));
          Get toGet = new Get(rowKey).addColumn(RelationWdeRefsHTable.FAMILY, EMPTY_QUALIFIER);
          // toGet.setTimeStamp( tsRangeStart);
          Result res = relWdeRefsTable.get(toGet);
          byte[] oldWdeRefSetBytes = res.getValue(RelationWdeRefsHTable.FAMILY, EMPTY_QUALIFIER);
          SortedWdeRefSet refSet = new SortedWdeRefSet(oldWdeRefSetBytes);
          int oldSize = refSet.size();
          refSet.add(entry.getValue());
          if (refSet.size() != oldSize) {// New WdeRef has been added,
            // we need to put it back
            Put toPut = new Put(rowKey).add(RelationWdeRefsHTable.FAMILY, EMPTY_QUALIFIER,
                refSet.getBytes());
            success = relWdeRefsTable.checkAndPut(rowKey, RelationWdeRefsHTable.FAMILY,
                EMPTY_QUALIFIER, oldWdeRefSetBytes, toPut);
          } else {
            success = true;// No need to change
          }
        }
      }
    } finally {
      closeHTable(relWdeRefsTable);
    }
  }

  // 不再具有TS信息
  @Deprecated
  private void addRelationWdeRefsOld(Relation rel) throws IOException {
    Edge relParent = rel.getParentEdge();
    HTableInterface relWdeRefsTable = pool.getRelationWdeRefsTable(rel.getParentEdgeHeadNodeType());
    try {
      List<WdeRef> refListToAdd = rel.getWdeRefs();
      byte[] rowKey = Bytes.add(relParent.getId(), rel.getType().getBytesForm());
      boolean success = false;
      while (!success) {// Try Get() and checkAndPut() until success
        // Try Get() and checkAndPut() until success
        Get toGet = new Get(rowKey).addColumn(RelationWdeRefsHTable.FAMILY, EMPTY_QUALIFIER);
        Result res = relWdeRefsTable.get(toGet);
        byte[] oldWdeRefSetBytes = res.getValue(RelationWdeRefsHTable.FAMILY, EMPTY_QUALIFIER);
        SortedWdeRefSet refSet = new SortedWdeRefSet(oldWdeRefSetBytes);
        int oldSize = refSet.size();
        refSet.add(refListToAdd);
        if (refSet.size() != oldSize) {// New WdeRef has been added,
          // we need to put it back
          Put toPut = new Put(rowKey).add(RelationWdeRefsHTable.FAMILY, EMPTY_QUALIFIER,
              refSet.getBytes());
          success = relWdeRefsTable.checkAndPut(rowKey, RelationWdeRefsHTable.FAMILY,
              EMPTY_QUALIFIER, oldWdeRefSetBytes, toPut);
        } else {
          success = true;// No need to change
        }

      }
    } finally {
      closeHTable(relWdeRefsTable);
    }

  }

  /**
   * Group the WdeRef List according to timestamp range.
   * 
   * @param refList
   * @return a Map from "timestamp range start" to "WdeRefs in this timestamp range"
   */
  private Map<Integer, List<WdeRef>> groupWdeRefList(List<WdeRef> refList) {
    Map<Integer, List<WdeRef>> wdeRefMap = new HashMap<Integer, List<WdeRef>>();
    for (WdeRef ref : refList) {
      int tsRangeStart = getTsRangeStart(ref.getTimestamp());
      List<WdeRef> list = wdeRefMap.get(tsRangeStart);
      if (list == null) {
        list = new ArrayList<WdeRef>();
        wdeRefMap.put(tsRangeStart, list);
      }
      list.add(ref);
    }
    return wdeRefMap;
  }

  private int getTsRangeStart(int ts) {
    return ts <= 0 ? 0 : ts - ts % GdbHTableConstant.TIME_GRANULARITY;
  }

  /**
   * Close an HTable and log the Exception if any.
   */
  private void closeHTable(HTableInterface htable) {
    if (htable == null) return;
    try {
      htable.close();
    } catch (IOException e) {
      LOG.error("Fail to close HTable: " + Bytes.toString(htable.getTableName()), e);
    }
  }

  /**
   * Get Relations in a given Edge
   * 
   * @param edge
   * @param relTypeConstraint
   * @param timeRange
   * @param loadWdeRefs
   * @return List<Pair<Integer,Relation>> 该边的时间和relation的pair,如果关系没有时间信息,则时间为0;
   * @throws GdbException
   */
  public List<Pair<Integer, List<Relation>>> getEdgeRelations(final Edge edge,
      RelationType relTypeConstraint, TimeRange timeRange, boolean loadWdeRefs) throws GdbException {
    System.out.println("edge Id : " + StringUtils.byteToHexString(edge.getId()));
    final List<Pair<Integer, List<Relation>>> relations = new ArrayList<Pair<Integer, List<Relation>>>();
    if (timeRange == null || timeRange.equals(TimeRange.ANY_TIME)) {
      byte[] rowKey = getSaltedRowKey(edge.getId());
      HTableInterface relWeightSumTable = pool.getEdgeRelWeightSumTable(edge.getHeadNodeType());
      try {
        Get toGet = new Get(rowKey);
        Result result = relWeightSumTable.get(toGet);
        List<Relation> relationL = new ArrayList<Relation>();
        for (KeyValue kv : result.raw()) {
          RelationType relT = RelationType.getType(Bytes.toString(kv.getQualifier()));
          // TODO relTypeConstraint
          long weight = Bytes.toLong(kv.getValue());
          Relation rel = new Relation(edge, relT);
          rel.setWeight((int) weight);
          // TODO loadWdeRefs
          // edge.addRelation(relT, (int) weight);
          relationL.add(rel);

        }
        relations.add(new Pair<Integer, List<Relation>>(0, relationL));
      } catch (IOException e) {
        throw new GdbException(e);
      } finally {
        closeHTable(relWeightSumTable);
      }
    } else {
      final HTableInterface relWeightDetailTable = pool.getEdgeRelWeightDetailTable(edge
          .getHeadNodeType());
      try {
        ParallelTask<List<Pair<Integer, List<Relation>>>> pTask = new ParallelTask<List<Pair<Integer, List<Relation>>>>(
            exec) {
          @Override
          public void processResult(List<Pair<Integer, List<Relation>>> result) {
            relations.addAll(result);

          }
        };
        final byte[] startRow = Bytes.add(edge.getId(),
            Bytes.toBytes((int) timeRange.getStartInclusiveInSec()));
        final byte[] endRow = Bytes.add(edge.getId(),
            Bytes.toBytes((int) timeRange.getEndExclusiveInSec()));

        System.out.println("startRow  : " + StringUtils.byteToHexString(startRow));
        System.out.println("endRow  : " + StringUtils.byteToHexString(endRow));
        for (byte salt = 0; salt < SALT_PARTITION_COUNT; salt++) {
          final byte saltlocal = salt;
          pTask.submitTasks(new Callable<List<Pair<Integer, List<Relation>>>>() {
            @Override
            public List<Pair<Integer, List<Relation>>> call() throws Exception {
              List<Pair<Integer, List<Relation>>> localRes = new ArrayList<Pair<Integer, List<Relation>>>();
              byte[] startRowLocal = Bytes.add(new byte[] { saltlocal }, startRow);
              byte[] endRowLocal = Bytes.add(new byte[] { saltlocal }, endRow);
              System.out.println("salt:" + saltlocal + " startRowLocal  : "
                  + StringUtils.byteToHexString(startRowLocal));
              System.out.println("salt:" + saltlocal + " endRowLocal  : "
                  + StringUtils.byteToHexString(endRowLocal));
              Scan scan = new Scan();
              scan.setStartRow(startRowLocal);
              scan.setStopRow(endRowLocal);
              ResultScanner scanner = relWeightDetailTable.getScanner(scan);
              for (Result res : scanner) {
                int time = Bytes.toInt(res.getRow(), res.getRow().length - 4, 4);
                List<Relation> relationL = new ArrayList<Relation>();
                for (KeyValue kv : res.raw()) {
                  RelationType relT = RelationType.getType(Bytes.toString(kv.getQualifier()));
                  // TODO relTypeConstraint
                  long weight = Bytes.toLong(kv.getValue());
                  Relation rel = new Relation(edge, relT);
                  rel.setWeight((int) weight);
                  // TODO loadWdeRefs
                  // edge.addRelation(relT, (int) weight);
                  relationL.add(rel);
                }
                localRes.add(new Pair<Integer, List<Relation>>(time, relationL));
              }
              System.out.println("salt:" + saltlocal + " size= " + localRes.size());
              return localRes;
            }
          });
        }

        pTask.gatherResults();

      } catch (Exception e) {
        throw new GdbException(e);
      } finally {
        closeHTable(relWeightDetailTable);
      }
    }
    List<Pair<Integer, List<Relation>>> relations1 = new ArrayList<Pair<Integer, List<Relation>>>();
    relations1.addAll(relations);
    Collections.sort(relations1, new Comparator<Pair<Integer, List<Relation>>>() {

      @Override
      public int compare(Pair<Integer, List<Relation>> o1, Pair<Integer, List<Relation>> o2) {
        return o2.getFirst() - o1.getFirst();
      }

    });
    return relations1;
  }

  public void cleanEdgeTableByTS(Channel channel, TimeRange timeRange) {
    cleanRelationTypeHTableByTS(channel, timeRange);
    cleanEdgeIdAndEdgeSumTableByTs(channel, timeRange);
  }

  /**
   * Get Relations in a given Edge
   * 
   * @param edge
   * @param relTypeConstraint
   * @param timeRange
   * @param loadWdeRefs
   * @return
   * @throws GdbException
   */
  public Edge getEdgeRelationsTMP(Edge edge, RelationType relTypeConstraint, TimeRange timeRange,
      boolean loadWdeRefs) throws GdbException {
    if (timeRange == null || timeRange.equals(TimeRange.ANY_TIME)) {
      byte[] rowKey = getSaltedRowKey(edge.getId());
      HTableInterface relWeightSumTable = pool.getEdgeRelWeightSumTable(edge.getHeadNodeType());
      try {
        Get toGet = new Get(rowKey);
        Result result = relWeightSumTable.get(toGet);
        for (KeyValue kv : result.raw()) {
          RelationType relT = RelationType.getType(Bytes.toString(kv.getQualifier()));
          // TODO relTypeConstraint
          long weight = Bytes.toLong(kv.getValue());
          // TODO loadWdeRefs
          edge.addRelation(relT, (int) weight);
        }
      } catch (IOException e) {
        throw new GdbException(e);
      } finally {
        closeHTable(relWeightSumTable);
      }
      return edge;
    } else {
      // TODO TimeRange constraint
      throw new UnsupportedOperationException();
    }
  }

  // edges
  public List<Edge> getEdgesRelations(List<Edge> edges, RelationType relTypeConstraint,
      TimeRange timeRange, boolean loadWdeRefs) throws GdbException {
    if (timeRange == null || timeRange.equals(TimeRange.ANY_TIME)) {
      Map<String, List<Edge>> edgesInOneTable = new HashMap<String, List<Edge>>();
      for (Edge edge : edges) {
        String tableName = EdgeRelWeightSumHTable.getName(edge.getHeadNodeType());
        List<Edge> edges1 = edgesInOneTable.get(tableName);
        if (edges1 == null) {
          edges1 = new ArrayList<Edge>();
          edgesInOneTable.put(tableName, edges1);
        }
        edges1.add(edge);
      }
      for (Entry<String, List<Edge>> e : edgesInOneTable.entrySet()) {
        getEdgesRelations(pool.getEdgeRelWeightSumTable(e.getValue().get(0).getHeadNodeType()),
            e.getValue(), relTypeConstraint, timeRange, loadWdeRefs);
      }
      return edges;
    } else {
      // TODO TimeRange constraint
      throw new UnsupportedOperationException();
    }
  }

  private void getEdgesRelations(HTableInterface table, List<Edge> edges,
      RelationType relTypeConstraint, TimeRange timeRange, boolean loadWdeRefs) throws GdbException {
    List<Row> batch = new ArrayList<Row>();
    byte[] family = EdgeRelWeightSumHTable.FAMILY;
    for (Edge edge : edges) {
      Get get = new Get(getSaltedRowKey(edge.getId()));
      get.addFamily(family);
      batch.add(get);
    }
    Object[] results = new Object[batch.size()];
    try {
      table.batch(batch, results);
      int i = 0;
      for (Edge edge : edges) {
        Result result = (Result) results[i++];
        for (KeyValue kv : result.raw()) {
          RelationType relT = RelationType.getType(Bytes.toString(kv.getQualifier()));
          // TODO relTypeConstraint
          long weight = Bytes.toLong(kv.getValue());
          // TODO loadWdeRefs
          edge.addRelation(relT, (int) weight);
        }
      }
    } catch (IOException e) {
      throw new GdbException(e);
    } catch (InterruptedException e) {
      throw new GdbException(e);
    }
  }

  /**
   * Get detail info of a Relation<br>
   * 
   * @param rel
   * @return
   * @throws GdbException
   */
  public Relation getRelationDetail(Relation rel) throws GdbException {
    byte[] relId = rel.getId();
    PrefixFilter filter = new PrefixFilter(relId);
    Scan scan = new Scan();
    scan.setStartRow(relId);
    scan.setFilter(filter);
    scan.setCaching(1000);
    // get.setMaxVersions();//to get all versions!
    scan.addColumn(RelationWdeRefsHTable.FAMILY, EMPTY_QUALIFIER);
    HTableInterface htable = pool.getRelationWdeRefsTable(rel.getParentEdgeHeadNodeType());
    try {
      ResultScanner scanner = htable.getScanner(scan);
      List<WdeRef> totalWdeRefs = new ArrayList<WdeRef>();
      SortedWdeRefSet totalSet = new SortedWdeRefSet();
      for (Result result : scanner) {
        byte[] row = result.getRow();
        if (row.length - relId.length != 4) continue;
        for (KeyValue kv : result.raw()) {
          totalSet.add(new SortedWdeRefSet(kv.getValue()).getList());
          ;
        }
      }
      totalWdeRefs = totalSet.getList();
      rel.setWdeRefs(totalWdeRefs);
      return rel;
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(htable);
    }
  }

  /**
   * Query a PathGraph with the spec
   * 
   * @param spec
   * @return
   */
  public PathGraph queryPathGraph(PathQuerySpec spec) throws GdbException {
    if (spec == null) throw new NullPointerException("null spec");
    PathGraph graph;
    try {
      graph = new GdbPathSearcher(spec, this, exec).search();
      return graph;
    } catch (Exception e) {
      throw new GdbException(e);
    }
  }

  /**
   * Expand the given Node. Get related Nodes' Ids according to spec, mainly used in Path search<br>
   * Do not use parallel Scans here, because there'll be many concurrent callers.
   * 
   * TODO: currently implementation: 1)support maxPathLength 2)requiredNodeType must be the same
   * with start Node type 3)requiredRelType not supported
   * 
   * 
   * @param spec
   * @return
   * @throws IOException
   */
  protected List<byte[]> getRelatedNodeIdList(Node nodeToExpand, PathQuerySpec spec)
      throws Exception {
    byte[] scanPrefix = Bytes.add(nodeToExpand.getId(), spec.getRequiredAttribute().getByteFrom());
    if (!needFilterEdges(spec)) {
      // Scan to get possible Edge id list
      List<byte[]> edgeIds = scanForEdgeIds(scanPrefix,
          NodeType.getType(nodeToExpand.getType().getChannel(), spec.getRequiredAttribute()));
      // Do not need to query Edge info.
      List<byte[]> nodeIdList = new ArrayList<byte[]>(edgeIds.size());
      for (byte[] edgeId : edgeIds) {
        // TODO: optimization- ByteArray with offset to avoid copy here
        nodeIdList.add(Bytes.tail(edgeId, edgeId.length / 2));
      }
      return nodeIdList;
    } else {// Filter the Edges
      // First, salt edge Ids to different partitions
      List<Scan> saltedScanList = buildPrefixScansForAllPartitions(Bytes.add(nodeToExpand.getId(),
          spec.getRequiredAttribute().getByteFrom()));
      // buildSaltedPartitionScans(edgeIds, 100000);// deprecated
      final RelationType requiredRelType = spec.getRequiredRelType();
      if (spec.getTimeRange().equals(TimeRange.ANY_TIME)) { // No time
        // constraints
        List<byte[]> nodeIdList = new ArrayList<byte[]>();

        // TODO do we need ANY_PERSON support here? see queryRelationGraph(RelQuerySpec) for how to
        // deal with ANY_PERSON
        HTableInterface htable = pool.getEdgeRelWeightSumTable(nodeToExpand.getType());// change
        // this to support ANY_PERSON
        try {
          for (final Scan scan : saltedScanList) {// sequential Scan in each partition
            ResultScanner results = htable.getScanner(scan);
            try {
              for (Result rowResult : results) {// for each Edge
                byte[] edgeId = getEdgeIdFromSaltedRowKey(rowResult.getRow());
                byte[] tailId = Bytes.tail(edgeId, edgeId.length / 2);
                boolean edgeExist = false;
                for (KeyValue kv : rowResult.raw()) {// for each Relation column
                  RelationType relType = RelationType.getType(Bytes.toString(kv.getQualifier()));
                  // Relation type constraints
                  if (requiredRelType == null
                      || (requiredRelType != null && requiredRelType == relType)) {
                    edgeExist = true;
                    break;// if the Edge exist, that's enough!
                  }
                  // TODO Edge weight constrains
                }
                if (edgeExist) {
                  nodeIdList.add(tailId);
                }
              }
            } finally {
              results.close();
            }
          }
        } finally {
          closeHTable(htable);
        }
        return nodeIdList;
      } else { // need time constraints.
        // TODO TimeRange constraints
        throw new UnsupportedOperationException("TimeRange unsupported.");
      }
    }
  }

  /**
   * Decide if we need to do filtering on Edges in Path query.
   */
  private boolean needFilterEdges(PathQuerySpec spec) {
    return !(spec.getRequiredRelType() == null && spec.getTimeRange() == TimeRange.ANY_TIME
        && spec.getMaxEdgeWeight() == Integer.MAX_VALUE && spec.getMinEdgeWeight() == 1);
  }

  /**
   * Query a given Node's all relation data.
   * This method is prepared for Gdb Cache to fetch data from HBase.
   * 
   * @return
   *         The returned 2-level Map is interpreted as:
   *         relation type ==>
   *         node attribute ==> List < Pair(related Node id, weight for this relation) >
   * @throws GdbException
   */
  public Map<String, Map<Attribute, List<Pair<byte[], Integer>>>> queryOneNodeContentForCache(
      byte[] nodeid) throws GdbException {
    final Map<String, Map<Attribute, List<Pair<byte[], Integer>>>> result = MapTool.newHashMap();
    final Node center = new Node(nodeid);
    // Triplet = < related node id, relation type string, weight >
    ParallelTask<List<Triplet<byte[], String, Integer>>> task = new ParallelTask<List<Triplet<byte[], String, Integer>>>(
        exec) {
      @Override
      public void processResult(List<Triplet<byte[], String, Integer>> part) {
        // Merge triplets into result map
        for (Triplet<byte[], String, Integer> triplet : part) {
          Map<Attribute, List<Pair<byte[], Integer>>> map = result.get(triplet.getSecond());
          if (map == null) {
            map = MapTool.newHashMap();
            result.put(triplet.getSecond(), map);
          }
          NodeType nt = Node.getType(triplet.getFirst());
          if (nt == null) {
            throw new IllegalArgumentException("Fail to get NodeType from Node id: "
                + Hex.encodeHex(triplet.getFirst()));
          }
          List<Pair<byte[], Integer>> list = map.get(nt.getAttribute());
          if (list == null) {
            list = new ArrayList<Pair<byte[], Integer>>();
            map.put(nt.getAttribute(), list);
          }
          list.add(Pair.newPair(triplet.getFirst(), triplet.getThird()));
        }
      }
    };
    List<Scan> scanList = buildPrefixScansForAllPartitions(nodeid);
    for (final Scan scan : scanList) {// Parallel scan in each partition
      task.submitTasks(new Callable<List<Triplet<byte[], String, Integer>>>() {
        @Override
        public List<Triplet<byte[], String, Integer>> call() throws Exception {
          List<Triplet<byte[], String, Integer>> list = new ArrayList<Triplet<byte[], String, Integer>>();
          HTableInterface relHTable = pool.getEdgeRelWeightSumTable(center.getType());
          try {
            ResultScanner results = relHTable.getScanner(scan);
            try {
              for (Result rowResult : results) {// for each edge
                byte[] relatedNodeId = Bytes.tail(rowResult.getRow(), Node.NODEID_SIZE);
                for (KeyValue kv : rowResult.raw()) {// for each Relation column
                  // relation types are limited, call intern() for memory efficiency
                  String relStr = Bytes.toString(kv.getQualifier()).intern();
                  int weight = (int) Bytes.toLong(kv.getValue());// Counter value is "long"
                  list.add(new Triplet<byte[], String, Integer>(relatedNodeId, relStr, weight));
                }
              }
            } finally {
              results.close();
            }
          } finally {
            closeHTable(relHTable);
          }
          return list;
        }
      });
    }
    try {
      task.gatherResults();
    } catch (Exception e) {
      throw new GdbException(e);
    }
    return result;
  }

  /**
   * 
   * @param spec
   *          Suppose center Node's id is available.
   * @return
   * @throws GdbException
   */
  public RelationGraph queryRelationGraph(final RelQuerySpec spec) throws GdbException {
    if (spec == null) throw new NullPointerException("null RelQuerySpec");
    RelationGraph relGraph;
    /*
     * First, construct a coarse RelationGraph
     */
    if (spec.getContainedWdeIds() != null) {
      relGraph = queryRelGraphInRelationWdeRefsTableDetail(spec);
    } else if (spec.getTimeRange().equals(TimeRange.ANY_TIME)) {
      if (spec.getRequiredRelType() == null && !spec.isRelRankEnabled()) {
        // No need for time info, relation info, nor relation weight
        // info.
        // So just scan results in EdgeID table!

        // System.out.println(spec.getRequiredNodeType().getIntegerForm());
        relGraph = queryRelGraphInEdgeIdTable(spec);

      } else {
        // We need relation info but no time info, so parallel scan in
        // EdgeRelWeightSum table.
        relGraph = queryRelGraphInEdgeRelWeightSumTable(spec);
      }
    } else { // Need time constraints, so query in EdgeRelWeightDetail
      // table.
      // TODO test!!!!!!
      relGraph = queryRelGraphInEdgeRelWeightDetailHTableDetail(spec);

      // throw new
      // UnsupportedOperationException("TimeRange is not supported :(");
    }
    // relGraph is now ready for following operations on it.

    /*
     * Second, Trim Edges to result size
     */
    if (!spec.isRelRankEnabled()) {
      relGraph = trimGraphEdgesToSizeRandomly(relGraph, spec.getResultSize());
    } else {
      RelationGraph sortedRelGraph = new RelationGraph(spec.getCenterNode());
      ArrayList<Edge> sortedEdges = new ArrayList<Edge>(relGraph.getCenterEdges());
      Collections.sort(sortedEdges, new Comparator<Edge>() {
        @Override
        public int compare(Edge e1, Edge e2) {

          // in descending order
          int flag = e2.getEdgeWeight() - e1.getEdgeWeight();
          if (flag == 0) flag = Bytes.compareTo(e1.getTail().getId(), e2.getTail().getId());// in
          // ascending
          // order.
          return flag;
        }
      });
      // retain resultSize Edges with top relation weights.
      int count = 0;
      for (Edge edge : sortedEdges) {
        sortedRelGraph.addCenterEdge(edge);
        count++;
        if (count == spec.getResultSize()) break;
      }
      relGraph = sortedRelGraph;

      // throw new
      // UnsupportedOperationException("Relation weight rank is not supported :(");
    }
    return relGraph;
  }

  public Map<String,byte[]> getRelationTypes(Channel channel) throws GdbException{
    Map<String,byte[]> results = new HashMap<String,byte[]>();
    HTableInterface table = pool.getRelationTypeTable(Channel.KNOWLEDGE);
    Filter filter = new PrefixFilter(channel.getByteFrom());
    Scan scan = new Scan();
    scan.setFilter(filter).setCaching(100);// TODO
    // scan.setFilter(filter).setCaching(10);
    scan.setStartRow(channel.getByteFrom());// for performance
    ResultScanner scanner = null;
    try {
      scanner = table.getScanner(scan);
      for(Result result : scanner){
        results.put( new String(Bytes.tail(result.getRow(), result.getRow().length-1)), result.getValue(RelationTypeHTable.FAMILY, RelationTypeHTable.QUALIFIER));
      }
    } catch (IOException e) {
      throw new GdbException(e);
    }finally{
      closeHTable(table);
    }
   return results;
  }
  public Map<String, byte[]> getRelationType(Channel channel) {
    HTableInterface table = pool.getEdgeRelWeightSumTable(NodeType.getType(channel,
        Attribute.PERSON));
    // TODO table not exits;
    Map<byte[], MapWritable> results = null;
    try {
      results = table.coprocessorExec(HBaseEdgeDaoProtocol.class, null, null,
          new Batch.Call<HBaseEdgeDaoProtocol, MapWritable>() {
            public MapWritable call(HBaseEdgeDaoProtocol instance) {
              try {
                return instance.getRelationType();
              } catch (Exception e) {

                // e.printStackTrace();
              }
              return null;
            }
          });
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Throwable e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Map<String, byte[]> relationTypes = new HashMap<String, byte[]>();
    for (Map.Entry<byte[], MapWritable> e : results.entrySet()) {
      for (Entry<Writable, Writable> e1 : e.getValue().entrySet()) {
        BytesWritable value = (BytesWritable) e1.getValue();
        relationTypes.put(e1.getKey().toString(),Bytes.head(value.getBytes(), value.getLength()));
      }
    }
    return relationTypes;
  }

  private RelationGraph queryRelGraphInEdgeRelWeightDetailHTable(RelQuerySpec spec)
      throws GdbException {
    final RelQuerySpecWritable specWritable = new RelQuerySpecWritable(spec);
    Map<byte[], Result> results = null;
    final Node centerNode = spec.getCenterNode();
    final RelationGraph relGraph = new RelationGraph(spec.getCenterNode());
    HTableInterface table = pool.getEdgeRelWeightDetailTable(centerNode.getType());
    try {
      long start = Timer.now();
      results = table.coprocessorExec(HBaseEdgeDaoProtocol.class, null, null,
      // 该Result和HBase的Result虽然是同一个类，但是意义是不同的，这个Result并不代表一个Row，而仅仅是一系列的KV
          new Batch.Call<HBaseEdgeDaoProtocol, Result>() {
            public Result call(HBaseEdgeDaoProtocol instance) {
              try {
                return instance.queryResultInEdgeRelWeightDetailHTable(specWritable);
              } catch (Exception e) {

                // e.printStackTrace();
              }
              return null;
            }
          });

      LOG.info(Bytes.toString(table.getTableName()) + ":Get Result in " + Timer.msSince(start)
          + "ms");

      int count = 0;
      start = Timer.now();
      // 下面对edgeList进行排序,用于edge去重.
      List<Edge> edgeList = new ArrayList<Edge>();
      for (Entry<byte[], Result> e : results.entrySet()) {
        if (e.getValue() != null) for (KeyValue kv : e.getValue().raw()) {
          Edge preEdge = new Edge(centerNode, new Node(kv.getRow()));
          preEdge.setEdgeWeight(Bytes.toInt(kv.getQualifier()));
          edgeList.add(preEdge);
          count++;
        }

      }
      Collections.sort(edgeList, new Comparator<Edge>() {
        @Override
        public int compare(Edge e1, Edge e2) {
          return Bytes.compareTo(e1.getTail().getId(), e2.getTail().getId());// in ascending
          // order.
        }
      });
      LOG.info(Bytes.toString(table.getTableName()) + ":Get Result size: " + count);
      // 去重.由于salt的存在.导致返回的结果可能有重复,需要把各个相同edge的weight相加,并去重.
      List<Edge> newedgeList = new ArrayList<Edge>();
      Edge preEdge = null;
      for (Edge edge : edgeList) {
        if (preEdge == null) {
          preEdge = edge;
          continue;
        }
        if (Bytes.compareTo(edge.getTail().getId(), preEdge.getTail().getId()) == 0) {
          preEdge.addWeight(edge.getEdgeWeight());
        } else {
          newedgeList.add(preEdge);
          preEdge = edge;
        }

      }
      if (preEdge != null) newedgeList.add(preEdge);
      LOG.info(Bytes.toString(table.getTableName()) + ": edgeList size: " + edgeList.size()
          + " newedgeList size: " + newedgeList.size());
      LOG.info(Bytes.toString(table.getTableName()) + ": merge result in " + Timer.msSince(start)
          + "ms");
      relGraph.addCenterEdges(newedgeList);
    } catch (IOException e3) {
      throw new GdbException(e3);
    } catch (Throwable e3) {
      throw new GdbException(e3);
    } finally {
      closeHTable(table);
    }
    return relGraph;

  }

  private RelationGraph queryRelGraphInEdgeRelWeightDetailHTableDetail(RelQuerySpec spec)
      throws GdbException {
    final RelQuerySpecWritable specWritable = new RelQuerySpecWritable(spec);
    Map<byte[], MapWritable> results = null;
    final Node centerNode = spec.getCenterNode();
    final RelationGraph relGraph = new RelationGraph(spec.getCenterNode());
    HTableInterface table = pool.getEdgeRelWeightDetailTable(centerNode.getType());
    try {
      long start = Timer.now();
      results = table.coprocessorExec(HBaseEdgeDaoProtocol.class, null, null,
          new Batch.Call<HBaseEdgeDaoProtocol, MapWritable>() {
            public MapWritable call(HBaseEdgeDaoProtocol instance) {
              try {
                return instance.queryResultInEdgeRelWeightDetailHTableDetail(specWritable);
              } catch (Exception e) {

                // e.printStackTrace();
              }
              return null;
            }
          });

      LOG.info(Bytes.toString(table.getTableName()) + ":Get Result in " + Timer.msSince(start)
          + "ms");

      start = Timer.now();
      // 下面对edgeList进行排序,用于edge去重.
      
      List<Edge> edgeList = new ArrayList<Edge>();
      Map<ByteArray,Map<RelationType,Integer>> edgesResult = new HashMap<ByteArray,Map<RelationType,Integer>>();
      for (Entry<byte[], MapWritable> e : results.entrySet()) {
        if (e.getValue() != null) for (Entry<Writable, Writable> edges: e.getValue().entrySet()) {
          BytesWritable nodeIdWritable = (BytesWritable) edges.getKey();
          byte[] nodeIdTmp =Bytes.head(nodeIdWritable.getBytes(),nodeIdWritable.getLength());
          ByteArray nodeId = new ByteArray(nodeIdTmp);
          Map<RelationType,Integer> edgeDetail = edgesResult.get(nodeId);
          if(edgeDetail == null){
            edgeDetail = new HashMap<RelationType,Integer>();
            edgesResult.put(nodeId, edgeDetail);
          }
          for(Entry<Writable,Writable> relation : ((MapWritable)edges.getValue()).entrySet()){
            BytesWritable relationWritable = (BytesWritable) relation.getKey();
            RelationType relType = RelationType.getType(Bytes.toString(Bytes.head(relationWritable.getBytes(),relationWritable.getLength())));
            int weight = ((IntWritable)relation.getValue()).get();
            if(edgeDetail.containsKey(relType))
              edgeDetail.put(relType, edgeDetail.get(relType)+weight);
            else 
              edgeDetail.put(relType, weight);
          }
        }
      }
      
      LOG.info(Bytes.toString(table.getTableName()) + ":Get Result size: " + edgesResult.size());
      
      for(Entry<ByteArray,Map<RelationType,Integer>> e : edgesResult.entrySet()){
        Node tailNode = new Node(e.getKey().getBytes());
        Edge edge = new Edge(centerNode,tailNode);
        for(Entry<RelationType,Integer> relation : e.getValue().entrySet())
          edge.addRelation(relation.getKey(), relation.getValue());
        edgeList.add(edge);
      }
      relGraph.addCenterEdges(edgeList);
    } catch (IOException e3) {
      throw new GdbException(e3);
    } catch (Throwable e3) {
      throw new GdbException(e3);
    } finally {
      closeHTable(table);
    }
    return relGraph;

  }
  
  private RelationGraph queryRelGraphInRelationWdeRefsTable(final RelQuerySpec spec)
      throws GdbException {
    final RelQuerySpecWritable specWritable = new RelQuerySpecWritable(spec);
    Map<byte[], Result> results;
    final Node centerNode = spec.getCenterNode();
    final RelationGraph relGraph = new RelationGraph(spec.getCenterNode());
    HTableInterface table = pool.getRelationWdeRefsTable(centerNode.getType());
    // Parallel Scan in each partition of each table
    try {
      long start = Timer.now();

      results = table.coprocessorExec(HBaseEdgeDaoProtocol.class, centerNode.getId(), null,// 该Result
          // 和HBase的Result虽然是同一个类，但是意义是不同的，这个Result并不代表一个Row，而仅仅是一系列的KV
          new Batch.Call<HBaseEdgeDaoProtocol, Result>() {
            public Result call(HBaseEdgeDaoProtocol instance) throws IOException {

              return instance.queryRelGraphRelationWdeRefsHTable(specWritable);

            }
          });

      LOG.info(Bytes.toString(table.getTableName()) + ":Get Result in " + Timer.msSince(start)
          + "ms");
      int count = 0;
      start = Timer.now();
      for (Entry<byte[], Result> e : results.entrySet()) {
        if (e.getValue() != null) for (KeyValue kv : e.getValue().raw()) {
          Edge edge = new Edge(centerNode, new Node(kv.getRow()));
          edge.setEdgeWeight(Bytes.toInt(kv.getQualifier()));
          relGraph.addCenterEdge(edge);
          count++;
        }

      }
      LOG.info(Bytes.toString(table.getTableName()) + ":Get Result size: " + count);
    } catch (IOException e1) {
      throw new GdbException(e1);
    } catch (Throwable e1) {
      throw new GdbException(e1);
    } finally {
      closeHTable(table);
    }

    return relGraph;
  }

  private RelationGraph queryRelGraphInRelationWdeRefsTableDetail(final RelQuerySpec spec)
      throws GdbException {
    final RelQuerySpecWritable specWritable = new RelQuerySpecWritable(spec);
    Map<byte[], MapWritable> results;
    final Node centerNode = spec.getCenterNode();
    final RelationGraph relGraph = new RelationGraph(spec.getCenterNode());
    HTableInterface table = pool.getRelationWdeRefsTable(centerNode.getType());
    // Parallel Scan in each partition of each table
    try {
      long start = Timer.now();

      results = table.coprocessorExec(HBaseEdgeDaoProtocol.class, centerNode.getId(), null,// 该Result
          // 和HBase的Result虽然是同一个类，但是意义是不同的，这个Result并不代表一个Row，而仅仅是一系列的KV
          new Batch.Call<HBaseEdgeDaoProtocol, MapWritable>() {
            public MapWritable call(HBaseEdgeDaoProtocol instance) throws IOException {

              return instance.queryRelGraphRelationWdeRefsHTableDetail(specWritable);

            }
          });

      LOG.info(Bytes.toString(table.getTableName()) + ":Get Result in " + Timer.msSince(start)
          + "ms");
      start = Timer.now();
      List<Edge> edgeList = new ArrayList<Edge>();
      Map<ByteArray,Map<RelationType,Integer>> edgesResult = new HashMap<ByteArray,Map<RelationType,Integer>>();
      for (Entry<byte[], MapWritable> e : results.entrySet()) {
        if (e.getValue() != null) for (Entry<Writable, Writable> edges: e.getValue().entrySet()) {
          BytesWritable nodeIdWritable = (BytesWritable) edges.getKey();
          byte[] nodeIdTmp =Bytes.head(nodeIdWritable.getBytes(),nodeIdWritable.getLength());
          ByteArray nodeId = new ByteArray(nodeIdTmp);
          Map<RelationType,Integer> edgeDetail = edgesResult.get(nodeId);
          if(edgeDetail == null){
            edgeDetail = new HashMap<RelationType,Integer>();
            edgesResult.put(nodeId, edgeDetail);
          }
          for(Entry<Writable,Writable> relation : ((MapWritable)edges.getValue()).entrySet()){
            BytesWritable relationWritable = (BytesWritable) relation.getKey();
            RelationType relType = RelationType.getType(Bytes.toString(Bytes.head(relationWritable.getBytes(),relationWritable.getLength())));
            int weight = ((IntWritable)relation.getValue()).get();
            if(edgeDetail.containsKey(relType))
              edgeDetail.put(relType, edgeDetail.get(relType)+weight);
            else 
              edgeDetail.put(relType, weight);
          }
        }
      }
      
      LOG.info(Bytes.toString(table.getTableName()) + ":Get Result size: " + edgesResult.size());
      
      for(Entry<ByteArray,Map<RelationType,Integer>> e : edgesResult.entrySet()){
        Node tailNode = new Node(e.getKey().getBytes());
        Edge edge = new Edge(centerNode,tailNode);
        for(Entry<RelationType,Integer> relation : e.getValue().entrySet())
          edge.addRelation(relation.getKey(), relation.getValue());
        edgeList.add(edge);
      }
      relGraph.addCenterEdges(edgeList);
    } catch (IOException e1) {
      throw new GdbException(e1);
    } catch (Throwable e1) {
      throw new GdbException(e1);
    } finally {
      closeHTable(table);
    }

    return relGraph;
  }
  
  private RelationGraph queryRelGraphInEdgeRelWeightSumTable(final RelQuerySpec spec)
      throws GdbException {
    final Node centerNode = spec.getCenterNode();
    // First, build Scans we need in each table partition.
    List<Scan> scanList = buildPrefixScansForAllPartitions(centerNode.getId(),
        spec.getRequiredAttribute());

    // Second, execute the Scans in appropriate tables.
    final RelationGraph relGraph = new RelationGraph(spec.getCenterNode());
    ParallelTask<List<Edge>> pTask = new ParallelTask<List<Edge>>(exec) {
      @Override
      public void processResult(List<Edge> result) {
        relGraph.addCenterEdges(result);// add each thread's result to
        // final graph
      }
    };
    for (final Scan scan : scanList) {
      final HTableInterface table = pool.getEdgeRelWeightSumTable(centerNode.getType());
      // Parallel Scan in each partition of each table
      pTask.submitTasks(new Callable<List<Edge>>() {
        @Override
        public List<Edge> call() throws IOException {
          List<Edge> edgeList = new ArrayList<Edge>();
          try {
            ResultScanner results = table.getScanner(new Scan(scan));
            try {
              for (Result rowResult : results) {// for each
                // Edge
                byte[] edgeId = getEdgeIdFromSaltedRowKey(rowResult.getRow());
                Edge edge = new Edge(centerNode, new Node(Bytes.tail(edgeId, edgeId.length / 2)));
                Set<RelationType> reqRelType = spec.getRequiredRelType();
                for (KeyValue kv : rowResult.raw()) {// for
                  // each
                  // Relation
                  // column
                  RelationType relType = RelationType.getType(Bytes.toString(kv.getQualifier()));
                  // Relation type constraints
                  if (reqRelType == null || (reqRelType != null && reqRelType.contains(relType))) {
                    Relation rel = new Relation(edge, relType);
                    // set Relation weight
                    rel.setWeight((int) Bytes.toLong(kv.getValue()));// Counter
                    // value is
                    // "long"
                    edge.addRelation(rel);
                  }
                }
                if (edge.getRelations().size() != 0) edgeList.add(edge);
              }
            } finally {
              results.close();
            }
            return edgeList;
          } finally {
            closeHTable(table);
          }
        }
      });
    }
    try {
      pTask.gatherResults();// gather results
    } catch (Exception e) {
      throw new GdbException(e);
    }
    return relGraph;
  }

  private RelationGraph queryRelGraphInEdgeIdTable(final RelQuerySpec spec) throws GdbException {
    RelationGraph relGraph = new RelationGraph(spec.getCenterNode());
    Node centerNode = spec.getCenterNode();
    byte[] scanPrefix = Bytes.add(centerNode.getId(), spec.getRequiredAttribute().getByteFrom());
    // Scan to get edge id list
    List<byte[]> edgeIds;
    try {
      edgeIds = scanForEdgeIds(scanPrefix, centerNode.getType(), spec.getResultSize());
    } catch (Exception e) {
      throw new GdbException(e);
    }
    // random select the required number of Edges.
    edgeIds = randomSelect(edgeIds, spec.getResultSize());
    for (byte[] edgeId : edgeIds) {
      Node tail = new Node(Bytes.tail(edgeId, edgeId.length / 2));
      Edge edge = new Edge(spec.getCenterNode(), tail);
      relGraph.addCenterEdge(edge);
    }
    return relGraph;
  }

  public long cleanRelationTypeHTableByTS(Channel channel, TimeRange timeRange) {
    if (timeRange == TimeRange.ANY_TIME) return 0L;// will not support ANY_TIME
    final byte[] start = Bytes.toBytes((int) timeRange.getStartInclusiveInSec());
    final byte[] end = Bytes.toBytes((int) timeRange.getEndExclusiveInSec());
    HTableInterface table = pool.getRelationWdeRefsTable(NodeType
        .getType(channel, Attribute.PERSON));
    Map<byte[], Long> results = null;
    long count = 0;
    try {
      long st = Timer.now();
      results = table.coprocessorExec(HBaseEdgeDaoProtocol.class, null, null,
          new Batch.Call<HBaseEdgeDaoProtocol, Long>() {
            public Long call(HBaseEdgeDaoProtocol instance) {
              try {
                return instance.cleanRelationTypeHTableByTS(start, end);
              } catch (Exception e) {

                e.printStackTrace();
              }
              return null;
            }
          });

      for (Map.Entry<byte[], Long> e : results.entrySet()) {
        count += e.getValue();
      }
      LOG.info(Bytes.toString(table.getTableName()) + ":delete complete in  " + Timer.msSince(st)
          + "ms" + ", the number of row delete :" + count + ". ");
    } catch (Throwable e) {
      e.printStackTrace();
    }
    return count;
  }

  public long cleanEdgeIdAndEdgeSumTableByTs(Channel channel, TimeRange timeRange) {

    if (timeRange == TimeRange.ANY_TIME) return 0L;// will not support ANY_TIME
    final byte[] start = Bytes.toBytes((int) timeRange.getStartInclusiveInSec());
    final byte[] end = Bytes.toBytes((int) timeRange.getEndExclusiveInSec());
    // HTableInterface relationTypeHTable =
    // pool.getEdgeRelWeightDetailTable(NodeType.getType(channel,
    // Attribute.PERSON));
    HTableInterface table = pool.getEdgeRelWeightDetailTable(NodeType.getType(channel,
        Attribute.PERSON));
    Map<byte[], Long> results = null;
    long count = 0;
    try {
      long st = Timer.now();
      results = table.coprocessorExec(HBaseEdgeDaoProtocol.class, null, null,
          new Batch.Call<HBaseEdgeDaoProtocol, Long>() {
            public Long call(HBaseEdgeDaoProtocol instance) {
              try {
                return instance.cleanEdgeRelWeightDetailHTableByTs1(start, end);
              } catch (Exception e) {

                e.printStackTrace();
              }
              return null;
            }
          });

      for (Map.Entry<byte[], Long> e : results.entrySet()) {
        count += e.getValue();
      }
      LOG.info(Bytes.toString(table.getTableName()) + ":delete complete in  " + Timer.msSince(st)
          + "ms" + ", the number of row delete :" + count + ". ");
    } catch (Throwable e) {
      e.printStackTrace();
    }
    return count;

  }

  /**
   * Drop some Edges in RelationGraph and return a NEW graph with resultSize Edges.(May be less than
   * resultSize if not enough Edges.)
   * 
   * @param relGraph
   * @param resultSize
   */
  private RelationGraph trimGraphEdgesToSizeRandomly(RelationGraph relGraph, int resultSize) {
    // If the Graph is already OK, return it.
    if (relGraph.getCenterEdges().size() <= resultSize) return relGraph;
    List<Edge> edges = new ArrayList<Edge>(relGraph.getCenterEdges());
    // Randomly drop!
    List<Edge> resultEdges = randomSelect(edges, resultSize);
    RelationGraph newGraph = new RelationGraph(relGraph.getCenterNode());
    newGraph.addCenterEdges(resultEdges);
    return newGraph;
  }

  /**
   * @param scanPrefix
   *          no salt byte needed.
   */
  private List<Scan> buildPrefixScansForAllPartitions(byte[] scanPrefix) {
    List<Scan> scanList = new ArrayList<Scan>(SALT_PARTITION_COUNT);
    for (byte salt = 0; salt < SALT_PARTITION_COUNT; salt++) {
      scanList.add(buildPrefixScan(Bytes.add(new byte[] { salt }, scanPrefix)));
    }
    return scanList;
  }

  /**
   * @param scanPrefix
   *          no salt byte needed.specify specific node type
   */
  public List<Scan> buildPrefixScansForAllPartitions(byte[] scanPrefix, Attribute attribute) {
    List<Scan> scanList = new ArrayList<Scan>(SALT_PARTITION_COUNT);
    for (byte salt = 0; salt < SALT_PARTITION_COUNT; salt++) {
      scanList.add(buildPrefixScan(Bytes.add(new byte[] { salt }, scanPrefix,
          attribute.getByteFrom())));
    }
    return scanList;
  }

  /**
   * @param scanPrefix
   *          no salt byte needed.
   */
  private List<Scan> buildPrefixScansForAllPartitions(byte[] scanPrefix, boolean keyOnly) {
    if (!keyOnly) return buildPrefixScansForAllPartitions(scanPrefix);
    else {
      List<Scan> scanList = new ArrayList<Scan>(SALT_PARTITION_COUNT);
      for (byte salt = 0; salt < SALT_PARTITION_COUNT; salt++) {
        scanList.add(buildPrefixKeyOnlyScan(Bytes.add(new byte[] { salt }, scanPrefix)));
      }
      return scanList;
    }
  }

  private static final Random rnd = new Random();

  /**
   * Select n elements from the given array randomly.<br>
   * If the array size is less than n, the array itself will be returned.
   * 
   * @param arr
   * @param resultSize
   * 
   * @return
   */
  private <T> List<T> randomSelect(List<T> arr, int resultSize) {
    int totalSize = arr.size();
    if (totalSize <= resultSize) return arr;
    for (int i = 0; i < resultSize; i++) {
      int sel = i + rnd.nextInt(totalSize - i);
      if (sel != i) {
        T t = arr.set(i, arr.get(sel));
        arr.set(sel, t);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Randomly select " + resultSize + " elements from " + arr.size() + " elements");
    }
    return arr.subList(0, resultSize);
  }

  /**
   * Distributed edgeIds to partitions.
   * 
   * @param edgeIds
   * @return
   */
  @Deprecated
  private List<Scan> buildSaltedPartitionScans(List<byte[]> edgeIds, int cachingSize) {
    // Get ids sorted. In fact, they should have been sorted.
    Collections.sort(edgeIds, Bytes.BYTES_COMPARATOR);
    Map<Byte, List<byte[]>> partitions = new HashMap<Byte, List<byte[]>>();
    for (byte[] edgeId : edgeIds) {
      byte salt = saltHash(edgeId);
      List<byte[]> sortedIdsInPartition = partitions.get(salt);
      sortedIdsInPartition = (sortedIdsInPartition == null) ? new ArrayList<byte[]>()
          : sortedIdsInPartition;
      sortedIdsInPartition.add(edgeId);
      partitions.put(salt, sortedIdsInPartition);
    }
    List<Scan> scanList = new ArrayList<Scan>(partitions.size());
    for (Entry<Byte, List<byte[]>> entry : partitions.entrySet()) {
      byte salt = entry.getKey();
      List<byte[]> ids = entry.getValue();
      Scan scan = new Scan();
      scan.setCaching(cachingSize);
      scan.setStartRow(Bytes.add(new byte[] { salt }, ids.get(0)));
      // add ZERO byte to stopRow to include the last id
      scan.setStopRow(Bytes.add(new byte[] { salt }, ids.get(ids.size() - 1), new byte[] { 0 }));
      scanList.add(scan);
    }
    return scanList;
  }

  /**
   * Scan Edge Id table(s) to get all required Edge ids.<br>
   * This method is used both in RelQuery and PathQuery.
   * 
   * @param scanPrefix
   *          Edge Id prefix for Scan
   * @param headType
   *          Edge head Node type
   * @return
   * @throws IOException
   */
  private List<byte[]> scanForEdgeIds(byte[] scanPrefix, NodeType headType) throws Exception {
    // First, build required Scan
    final Scan scan = buildPrefixScan(scanPrefix);
    // Second, get related HTables
    HTableInterface htable = pool.getEdgeIdTable(headType);
    // Scan in only one HTable

    return scanOneHTableForEdgeIds(htable, scan);
  }

  private List<byte[]> scanForEdgeIds(byte[] scanPrefix, NodeType headType, int limit)
      throws Exception {

    // First, build required Scan
    // final Scan scan = buildPrefixScan(scanPrefix);
    PrefixFilter filter = new PrefixFilter(scanPrefix);
    PageFilter filter1 = new PageFilter(limit);
    FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    filters.addFilter(filter1);
    filters.addFilter(filter);
    final Scan scan = new Scan();
    scan.setStartRow(scanPrefix);
    scan.setFilter(filters);
    int cachingSize = limit > 10000 ? 10000 : limit;
    scan.setCaching(cachingSize);// 这个应该还是挺重要的。
    // Second, get related HTables
    HTableInterface htable = pool.getEdgeIdTable(headType);
    return scanOneHTableForEdgeIds(htable, scan);
  }

  /**
   * Execute the given Scan in one HTable. The table will be closed before return.
   */
  private List<byte[]> scanOneHTableForEdgeIds(HTableInterface htable, Scan scan)
      throws IOException {
    List<byte[]> edgeIds = new ArrayList<byte[]>();
    try {
      ResultScanner results = htable.getScanner(scan);
      try {
        for (Result result : results) {
          edgeIds.add(result.getRow());
        }
      } finally {
        results.close();
      }
      return edgeIds;
    } finally {
      closeHTable(htable);
    }
  }

  private Scan buildPrefixScan(byte[] scanPrefix) {
    Filter filter = new PrefixFilter(scanPrefix);
    Scan scan = new Scan();
    scan.setFilter(filter).setCaching(10000);// TODO
    // scan.setFilter(filter).setCaching(10);
    scan.setStartRow(scanPrefix);// for performance
    return scan;
  }

  private Scan buildPrefixKeyOnlyScan(byte[] scanPrefix) {

    PrefixFilter prefilter = new PrefixFilter(scanPrefix);
    FirstKeyOnlyFilter keyOnlyfilter = new FirstKeyOnlyFilter();
    FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    fl.addFilter(keyOnlyfilter);
    fl.addFilter(prefilter);
    Scan scan = new Scan();
    scan.setFilter(fl).setCaching(10000);// TODO
    scan.setStartRow(scanPrefix);// for performance
    return scan;
  }

  public static void main(String[] args) {
    Random r = new Random();
    long[] distribution = new long[SALT_PARTITION_COUNT];
    byte[] bytes = new byte[50];
    r.nextBytes(bytes);
    long t = Timer.now();
    for (int i = 0; i < 3200000; i++) {
      r.nextBytes(bytes);
      distribution[saltHash(bytes)]++;
    }// ///
    System.out.println(Timer.msSince(t) + "ms");
    for (int i = 0; i < distribution.length; i++) {
      System.out.println(i + ":" + distribution[i]);
    }
  }
}
