package ict.ada.gdb.dao;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeAttribute;
import ict.ada.common.model.NodeAttribute.AttrValueInfo;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.common.model.WdeRef;
import ict.ada.common.util.Pair;
import ict.ada.common.util.Timer;
import ict.ada.common.util.Triplet;
import ict.ada.gdb.common.AdaConfig;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.SortedWdeRefSet;
import ict.ada.gdb.common.TimeRange;
import ict.ada.gdb.coprocessor.HBaseNodeDaoProtocol;
import ict.ada.gdb.schema.CommunityPersonRelHTable;
import ict.ada.gdb.schema.GdbHTableConstant;
import ict.ada.gdb.schema.GdbHTablePartitionPolicy;
import ict.ada.gdb.schema.LocationNodeTasksHTable;
import ict.ada.gdb.schema.NodeAttributeHTable;
import ict.ada.gdb.schema.NodeIdHTable;
import ict.ada.gdb.schema.NodeNameHTable;
import ict.ada.gdb.schema.NodeWdeRefsHTable;
import ict.ada.search.AdaSearchService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteProtocol;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteResponse;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.javatuples.Quartet;

public class HBaseNodeDAO {
  private static Log LOG = LogFactory.getLog(HBaseNodeDAO.class);

  private GdbHTablePool pool;
  private AdaSearchService searchService;
  // Set it 1 to minimize loss if something crashes when indexing
  final private int NO_OF_BATCHED_NODES = 1;
  private List<Quartet<String, NodeType, List<String>, String>> batchedNodeCache;
  private List<Quartet<NodeType, byte[], List<String>, List<String>>> batchedHbaseSnameCache;
  private List<Quartet<NodeType, byte[], List<String>, List<String>>> batchedHbaseAdditionalCache;
  private final ExecutorService exec;
  private static final HashSet<String> COMMUNITYNAME = new HashSet<String>();
  private static final String ACTION="action";
  static {
    COMMUNITYNAME.addAll(Arrays.asList("cm_hm"));
  }

  public HBaseNodeDAO(GdbHTablePool pool) {
    this.pool = pool;
    this.searchService =
        new AdaSearchService(AdaConfig.INDEX_SERVER_ADDR, AdaConfig.MQ_SERVER_ADDR,
            AdaConfig.MQ_NAME);
    batchedNodeCache =
        new ArrayList<Quartet<String, NodeType, List<String>, String>>(NO_OF_BATCHED_NODES);
    batchedHbaseSnameCache = new ArrayList<Quartet<NodeType, byte[], List<String>, List<String>>>();
    batchedHbaseAdditionalCache  = new  ArrayList<Quartet<NodeType, byte[], List<String>, List<String>>>();
    exec = Executors.newCachedThreadPool();// core pool
  }

  public byte[] addNode(Node node) throws GdbException {
    Boolean placeIsExists = false;
    String name = trimUnicodeHeader(node.getName());
    node.setName(name);
    node.setName(node.getType().getStringForm() + node.getName()); // 为name加上前缀!
    trimNodeSnameBOM(node);
    byte[] nodeId = addNodeImpl2(node, placeIsExists);
    if (node.getType().getAttribute() == Attribute.LOCATION && placeIsExists == false) {
      addLocationNodeTask(node);
    }
    return nodeId;
  }

  /*
   * Node Query
   */
  public Node getNodeAttributes(Node node,boolean loadInfo) throws GdbException {
    HTableInterface hiAttrTable = null;
    if (node.getId() != null) {
      NodeType type = getNodeTypeFromId(node.getId());
      hiAttrTable = pool.getNodeAttributeTable(type);
    } else if (node.getName() != null && node.getType() != null) {
      hiAttrTable = pool.getNodeAttributeTable(node.getType());
    }

    if (hiAttrTable == null) {
      throw new GdbException("null type in getNodeAttributes");
    }
    if (node.getId() != null) return getNodeAttributesById(hiAttrTable, node,loadInfo);
    if (node.getName() != null && node.getType() != null) {
      return getNodeAttributesByNameAndType(hiAttrTable, node,loadInfo);
    } else {
      throw new GdbException("name and type or node id must be specified.");
    }
  }

  /**
   * @param node
   * @param wdeIds<br>
   *        the attr's should come from one of wdeIds,and the format of wdeIds should be right
   * @return
   * @throws GdbException
   */
  public Node getNodeAttributes(Node node, List<byte[]> wdeIds) throws GdbException {
    HTableInterface hiAttrTable = null;
    if (node.getId() != null) {
      NodeType type = getNodeTypeFromId(node.getId());
      hiAttrTable = pool.getNodeAttributeTable(type);
    } else if (node.getName() != null && node.getType() != null) {
      hiAttrTable = pool.getNodeAttributeTable(node.getType());
    }

    if (hiAttrTable == null) {
      throw new GdbException("null type in getNodeAttributes");
    }
    if (node.getId() != null)
      getNodeAttributesById(hiAttrTable, node);
    else if (node.getName() != null && node.getType() != null) {
      getNodeAttributesByNameAndType(hiAttrTable, node,true);
    } else {
      throw new GdbException("name and type or node id must be specified.");
    }
    HashSet<ByteArray> wdeIdSet = new HashSet<ByteArray>();
    for (byte[] wdeid : wdeIds)
      wdeIdSet.add(new ByteArray(wdeid));
    Node tmpNode = new Node(node.getId());
    tmpNode.setSnames(node.getSnames());
    if (node.getName() != null) tmpNode.setNameAndType(node.getName(), node.getType());
    for (NodeAttribute attr : node.getAttributes()) {
      List<AttrValueInfo> attrInfos = new ArrayList<AttrValueInfo>();
      for (AttrValueInfo attrInfo : attr.getValues()) {
        List<WdeRef> wdeRefs = new ArrayList<WdeRef>();
        for (WdeRef wderef : attrInfo.getWdeRefs()) {
          if (wdeIdSet.contains(new ByteArray(wderef.getWdeId()))) wdeRefs.add(wderef);
        }
        if (wdeRefs.size() > 0) attrInfos.add(new AttrValueInfo(attrInfo.getValue(), wdeRefs));
      }
      if (attrInfos.size() > 0)
        tmpNode.addNodeAttribute(new NodeAttribute(attr.getKey(), attrInfos));
    }
    node = tmpNode;
    return node;
  }
public   List<NodeAttribute> getNodeAttrWdeRefs(Node node, List<NodeAttribute> attributes) throws GdbException{
	HTableInterface  hiAttrTable = pool.getNodeAttributeTable(node.getType());
	List<Get> gets=new ArrayList<Get>();
	for(NodeAttribute key:attributes){
		byte[] rowKey=Bytes.add(node.getId(), key.getKey().getBytes());
		for(AttrValueInfo value:key.getValues()){
			Get get =new Get(rowKey);
			get.addColumn(NodeAttributeHTable.FAMILY.getBytes(),value.getValue().getBytes() );
			//System.out.println(key.getKey()+" = "+ value.getValue());
			gets.add(get);
		}
	}
	Result[] results=new Result[gets.size()];
	List<NodeAttribute> attrWithWdeRefs=new ArrayList<NodeAttribute>(attributes.size());
	try {
		hiAttrTable.batch(gets,results);
		int index=0;
		for(NodeAttribute key:attributes){
			List<AttrValueInfo> values=new ArrayList<AttrValueInfo>(key.getValues().size());
			for(AttrValueInfo value:key.getValues()){
				List<WdeRef> wdeRefs=new SortedWdeRefSet(results[index++].getValue(NodeAttributeHTable.FAMILY.getBytes(),value.getValue().getBytes() )).getList();
				AttrValueInfo valueWithWdeRefs=new AttrValueInfo(value.getValue(),wdeRefs);
				values.add(valueWithWdeRefs);
			}
			NodeAttribute keyWithWdeRefs=new NodeAttribute(key.getKey(),values);
			attrWithWdeRefs.add(keyWithWdeRefs);
		}
		return attrWithWdeRefs;
	} catch (Exception e) {
		throw new GdbException(e);
	} 
}

public List<Pair<Integer,NodeAttribute>> getNodeActionAttrWdeRefs(Node node,String value,TimeRange timeRange) throws GdbException{
	byte[]start =null;
	byte[]end=null;
     if (!timeRange.equals(TimeRange.ANY_TIME)) {
       start = Bytes.toBytes(getTsRangeStartAction((int) timeRange.getStartInclusiveInSec()));
       end = Bytes.toBytes(getTsRangeStartAction((int) timeRange.getEndExclusiveInSec()));
     } else {
       start = Bytes.toBytes(0);
       end = Bytes.toBytes(Integer.MAX_VALUE);
     }
     byte[] startRow =Bytes.add(Bytes.add(node.getId(), ACTION.getBytes(),NodeAttributeHTable.DELIMITER), start) ;
     byte[] stopRow =Bytes.add(Bytes.add(node.getId(), ACTION.getBytes(),NodeAttributeHTable.DELIMITER), end) ;
     Scan scan =new Scan();
     scan.setStartRow(startRow);
     scan.setStopRow(stopRow);
     if(value != null && value.length() != 0)
     scan.addColumn(NodeAttributeHTable.FAMILY.getBytes(),value.getBytes());
	HTableInterface  hiAttrTable = pool.getNodeAttributeTable(node.getType());
	try {
		ResultScanner scanner =hiAttrTable.getScanner(scan);
		List<Pair<Integer,NodeAttribute>>attrs=new ArrayList<Pair<Integer,NodeAttribute>>();
		for(Result result:scanner ){
			List<AttrValueInfo> values=new ArrayList<AttrValueInfo>();
			for(KeyValue kv:result.raw()){
			     List<WdeRef> wdeRefs=new SortedWdeRefSet(kv.getValue()).getList();
			     values.add(new AttrValueInfo(Bytes.toString(kv.getQualifier()),wdeRefs));
			}
			NodeAttribute attr=new NodeAttribute(ACTION,values);
			int ts = Bytes.toInt(Bytes.tail(result.getRow(), 4));
			Pair<Integer,NodeAttribute> attrWithTs=new Pair<Integer,NodeAttribute>(ts,attr);
			attrs.add(attrWithTs);
		}
		return attrs;
	} catch (IOException e) {
	        throw new GdbException(e);
	} 
}

  /**
   * @param node
   * @param timeRange TimeRange should in hour level and range is [start , end)
   * @return
   * @throws GdbException
   */
  public Node getNodeWdeRefs(Node node, TimeRange timeRange) throws GdbException {
    byte[] start = null;
    byte[] end = null;
    Node retNode = getNodeAttributes(node,true);
    HTableInterface hiDetailTable = pool.getNodeWdeRefsTable(node.getType());
    try {
      byte[] id = retNode.getId();
      if (!timeRange.equals(TimeRange.ANY_TIME)) {
        start = Bytes.toBytes(getTsRangeStart((int) timeRange.getStartInclusiveInSec()));
        end = Bytes.toBytes(getTsRangeStart((int) timeRange.getEndExclusiveInSec()));
      } else {
        start = Bytes.toBytes(0);
        end = Bytes.toBytes(Integer.MAX_VALUE);
      }
      Scan scan = new Scan();
      scan.setStartRow(Bytes.add(id, start));
      scan.setStopRow(Bytes.add(id, end));// exclusive
      scan.setCaching(1000);
      ResultScanner rs = hiDetailTable.getScanner(scan);
      for (Result result : rs) {
        List<WdeRef> wdeRefs =
            (new SortedWdeRefSet(result.getValue(Bytes.toBytes(NodeWdeRefsHTable.FAMILY),
                Bytes.toBytes(NodeWdeRefsHTable.QUALIFIER)))).getList();
        retNode.addWdeRefs(wdeRefs);
      }
      return retNode;
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(hiDetailTable);
    }
  }


  /**
   * @param name
   * @param type
   * @return null if no id available
   * @throws GdbException
   */
  public byte[] getNodeIdByName(String name, NodeType type) throws GdbException {
    HTableInterface hiValueTable = pool.getNodeNameTable(type);
    try {
      Get get = new Get(name.getBytes("UTF-8"));
      get.addColumn(NodeNameHTable.FAMILY, NodeNameHTable.QUALIFIER);
      Result result = hiValueTable.get(get);
      return result.getValue(NodeNameHTable.FAMILY, NodeNameHTable.QUALIFIER);
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(hiValueTable);
    }
  }

  /**
   * @param id
   * @param type
   * @return null if no name available
   * @throws GdbException
   */
  public String getNodeNameById(byte[] id, NodeType type) throws GdbException {
    HTableInterface hiIdTable = pool.getNodeIdTable(type);
    try {
      Get get = new Get(id);
      get.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.QUALIFIER));
      Result result = hiIdTable.get(get);
      byte[] val =
          result
              .getValue(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.QUALIFIER));
      return Bytes.toString(val);
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(hiIdTable);
    }
  }

  public String getNodeNameById(byte[] id) throws GdbException {
    NodeType type = getNodeTypeFromId(id);
    HTableInterface hiIdTable = pool.getNodeIdTable(type);
    try {
      Get get = new Get(id);
      get.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.QUALIFIER));
      Result result = hiIdTable.get(get);
      byte[] val =
          result
              .getValue(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.QUALIFIER));
      return Bytes.toString(val);
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(hiIdTable);
    }
  }
  
  public List<String> getNodeEntsById1(byte[] id) throws GdbException{
	    NodeType type = getNodeTypeFromId(id);
	    HTableInterface hiIdTable = pool.getNodeIdTable(type);
	    try {
	    	byte[] entsBytes = getEnts(hiIdTable, id);
	    	List<String> result = new ArrayList<String>();
	    	if(entsBytes == null){
	    		return result;
	    	}else{
	    		String[] ents = Bytes.toString(entsBytes).split(SNAME_INTERNAL_SEP);
	    		for(String ent : ents){
	    			result.add(ent);
	    		}
	    	}
	    	return result;
	    } catch (IOException e) {
	      throw new GdbException(e);
	    } finally {
	      closeHTable(hiIdTable);
	    }
	  
  }
  /**
   * Batched get name by node id, all nodes must be in the same channel.
   * 
   * @param ids ids is a two dimensional byte array.
   * @return
   * @throws GdbException
   */
  public List<String> getNodeNameByIdBatched(List<byte[]> ids) throws GdbException {
    NodeType type = getNodeTypeFromId(ids.get(0)); // Get node type, all
    // node must be the same
    // type.
    HTableInterface hiIdTable = pool.getNodeIdTable(type);
    List<String> names = new ArrayList<String>();

    List<Row> batch = new ArrayList<Row>();
    for (int i = 0; i < ids.size(); i++) {
      Get get = new Get(ids.get(i));
      get.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.QUALIFIER));
      batch.add(get);
    }

    Object[] results = new Object[batch.size()];
    try {
      hiIdTable.batch(batch, results);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      for (int i = 0; i < results.length; i++) {
        byte[] val =
            ((Result) results[i]).getValue(Bytes.toBytes(NodeIdHTable.FAMILY),
                Bytes.toBytes(NodeIdHTable.QUALIFIER));
        names.add(Bytes.toString(val));
      }
    }

    return names;
  }

  public List<Pair<String, List<String>>> getNodeNameAndSnameByIdBatched(List<byte[]> ids)
      throws GdbException {
    byte[] family = Bytes.toBytes(NodeIdHTable.FAMILY);
    byte[] qualifier = Bytes.toBytes(NodeIdHTable.QUALIFIER);
    byte[] squalifier = Bytes.toBytes(NodeIdHTable.SQUALIFIER);
    NodeType type = getNodeTypeFromId(ids.get(0));
    HTableInterface hiIdTable = pool.getNodeIdTable(type);
    List<Pair<String, List<String>>> nameandsnameList = new ArrayList<Pair<String, List<String>>>();
    List<Row> batch = new ArrayList<Row>();
    for (int i = 0; i < ids.size(); i++) {
      Get get = new Get(ids.get(i));
      get.addFamily(family);
      batch.add(get);
    }
    Object[] results = new Object[batch.size()];
    try {
      hiIdTable.batch(batch, results);
      for (Object res : results) {
        Result result = (Result) res;
        String name = null;
        List<String> snames = new ArrayList<String>();
        byte[] nameb = result.getValue(family, qualifier);
        byte[] snameb = result.getValue(family, squalifier);
        if (nameb != null) name = Bytes.toString(nameb);
        if (snameb != null)
          Collections.addAll(snames, Bytes.toString(snameb).split(SNAME_INTERNAL_SEP));
        Pair<String, List<String>> pair = new Pair<String, List<String>>(name, snames);
        nameandsnameList.add(pair);
      }

    } catch (IOException e) {
      throw new GdbException(e);
    } catch (InterruptedException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(hiIdTable);
    }

    return nameandsnameList;
  }

  /**
   * Batched get name by node id, all nodes can be in the different channels.
   * 
   * @param ids ids is a two dimensional byte array.
   * @return
   * @throws GdbException
   */
  @Deprecated
  public Map<byte[], String> getNodeNameByIds(List<byte[]> ids) throws GdbException {
    if (ids.isEmpty()) return null;
    Map<NodeType, List<byte[]>> channelMap = new HashMap<NodeType, List<byte[]>>();
    for (byte[] id : ids) {
      NodeType type = getNodeTypeFromId(id);
      List<byte[]> idList = channelMap.get(type);
      if (idList == null) {
        idList = new ArrayList<byte[]>();
      }
      idList.add(id);
      channelMap.put(type, idList);
    }

    Map<byte[], String> idNames = new HashMap<byte[], String>();
    Iterator<Entry<NodeType, List<byte[]>>> it = channelMap.entrySet().iterator();
    while (it.hasNext()) {
      Entry<NodeType, List<byte[]>> entry = it.next();
      List<byte[]> idList = entry.getValue();
      Map<byte[], String> singleChannelIdNames = getNodeNameByIdBatchedInternal(idList);
      Iterator<Entry<byte[], String>> itSingleChannelIdNames =
          singleChannelIdNames.entrySet().iterator();
      while (it.hasNext()) {
        Entry<byte[], String> idNameEntry = itSingleChannelIdNames.next();
        byte[] id = idNameEntry.getKey();
        String name = idNameEntry.getValue();
        idNames.put(id, name);
      }
    }
    return idNames;
  }

  private void trimNodeSnameBOM(Node node) {
    if (node.getSnames() == null) return;
    List<String> newSnames = new ArrayList<String>();
    List<String> snames = node.getSnames();
    for (String sname : snames) {
      String newSname = trimUnicodeHeader(sname);
      newSnames.add(newSname);
    }
    node.setSnames(newSnames);
    List<String> newAdditionals = new ArrayList<String>();
    List<String> additionals = node.getAdditionals();
    for(String additional : additionals){
        String newAdditional = trimUnicodeHeader(additional);
        newAdditionals.add(newAdditional);
    }
    node.setAdditionals(newAdditionals);
    List<String> newEnts = new ArrayList<String>();
    for(String ent : node.getEnts()){
    	newEnts.add(trimUnicodeHeader(ent));
    }
    node.setEnts(newEnts);
  }

  private void modifyNodeName(Node node) {
    node.setName(node.getType().getStringForm() + node.getName());
  }

  private Map<byte[], String> getNodeNameByIdBatchedInternal(List<byte[]> ids) throws GdbException {
    NodeType type = getNodeTypeFromId(ids.get(0)); // Get node type, all
                                                   // node must be the same
                                                   // type.
    HTableInterface hiIdTable = pool.getNodeIdTable(type);
    Map<byte[], String> names = new HashMap<byte[], String>();

    List<Row> batch = new ArrayList<Row>();
    for (int i = 0; i < ids.size(); i++) {
      Get get = new Get(ids.get(i));
      get.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.QUALIFIER));
      batch.add(get);
    }

    Object[] results = new Object[batch.size()];
    try {
      hiIdTable.batch(batch, results);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      for (int i = 0; i < results.length; i++) {
        byte[] key = ((Result) results[i]).getRow();
        byte[] val =
            ((Result) results[i]).getValue(Bytes.toBytes(NodeIdHTable.FAMILY),
                Bytes.toBytes(NodeIdHTable.QUALIFIER));
        names.put(key, Bytes.toString(val));
      }
    }

    return names;
  }

  /**
   * get node name and sname by node id ,
   * 
   * @param nodeId
   * @return
   * @throws GdbException
   */
  public Node getNodeNameAndSnameById(byte[] nodeId) throws GdbException {
    Node node = new Node(nodeId);
    HTableInterface hiIdTable = pool.getNodeIdTable(node.getType()); // TODO
                                                                     // nodeId
                                                                     // is
                                                                     // illegal,
    Get get = new Get(nodeId);
    get.addFamily(Bytes.toBytes(NodeIdHTable.FAMILY));
    try {
      Result result = hiIdTable.get(get);
      byte[] name =
          result
              .getValue(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.QUALIFIER));
      byte[] sname =
          result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY),
              Bytes.toBytes(NodeIdHTable.SQUALIFIER));
      byte[] ents = result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.EQUALIFIER));
      if (name != null) node.setName(Bytes.toString(name));
      if (sname != null) {
        String[] snameArray = Bytes.toString(sname).split(SNAME_INTERNAL_SEP);
        for (String sName : snameArray)
          node.addSearchName(sName);
      }
      if(ents != null){
    	  String[] entArray = Bytes.toString(ents).split(SNAME_INTERNAL_SEP);
    	  for(String ent : entArray){
    		  node.addEnts(ent);
    	  }
      }
    } catch (Exception e) {
      // TODO Auto-generated catch block
      throw new GdbException(e);
    } finally {
      closeHTable(hiIdTable);
    }

    return node;

  }

  public Node getNodeEntsById(byte[] nodeId) throws GdbException{
	    Node node = new Node(nodeId);
	    HTableInterface hiIdTable = pool.getNodeIdTable(node.getType()); // TODO
	    Get get = new Get(nodeId);
	    get.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.EQUALIFIER));
	    try {
	      Result result = hiIdTable.get(get);
	      byte[] ents =
	          result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY),
	              Bytes.toBytes(NodeIdHTable.EQUALIFIER));
	      if (ents != null) {
	        String[] entArray = Bytes.toString(ents).split(SNAME_INTERNAL_SEP);
	        for (String ent : entArray)
	          node.addEnts(ent);
	      }
	    } catch (Exception e) {
	      throw new GdbException(e);
	    } finally {
	      closeHTable(hiIdTable);
	    }

	    return node;

	  
  }
  // public void deleteSearchNames(NodeType nodeType) throws IOException,
  // GdbException {
  // HTableInterface hiIdTable = pool.getNodeIdTable(nodeType);
  // Scan scan = new Scan();
  // ResultScanner resultScanner = hiIdTable.getScanner(scan);
  // for (Result result : resultScanner) {
  // byte[] rowkey = result.getRow();
  // doDeleteSearchNames(hiIdTable, rowkey);
  // }
  //
  // closeHTable(hiIdTable);
  // }

  // private void doDeleteSearchNames(HTableInterface hTableInterface, byte[]
  // rowkey)
  // throws GdbException {
  // Delete delete = new Delete(rowkey);
  // delete
  // .deleteColumns(Bytes.toBytes(NodeIdHTable.FAMILY),
  // Bytes.toBytes(NodeIdHTable.SQUALIFIER));
  // try {
  // hTableInterface.delete(delete);
  // } catch (IOException e) {
  // throw new GdbException("Unable to delete the specified row: " +
  // e.getMessage());
  // }
  // }

  public long deleteSearchNamesInParallel(Channel channel) throws GdbException {
    final Scan scan = new Scan();
    scan.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.SQUALIFIER));

    final int rowBatchSize = 200000;
    Batch.Call<BulkDeleteProtocol, BulkDeleteResponse> callable =
        new Batch.Call<BulkDeleteProtocol, BulkDeleteResponse>() {
          public BulkDeleteResponse call(BulkDeleteProtocol instance) throws IOException {
            return instance.delete(scan, BulkDeleteProtocol.DeleteType.COLUMN, null, rowBatchSize);
          }
        };

    /*
     * Do NOT use HTable pool here, because special timeout settings here may affect the pooled
     * tables! Instead, construct a separate HTable here.
     */
    Configuration conf = HBaseConfiguration.create();
    // Set rpc timeout to 10min, because CoProcessor operations in regions
    // take much time.
    conf.setLong("hbase.rpc.timeout", 600000);
    HTableInterface ht = null;
    long numOfDeletedRows = 0L;
    try {
      ht = new HTable(conf, NodeIdHTable.getName(channel));
      Map<byte[], BulkDeleteResponse> result =
          ht.coprocessorExec(BulkDeleteProtocol.class, scan.getStartRow(), scan.getStopRow(),
              callable);
      for (BulkDeleteResponse response : result.values()) {
        numOfDeletedRows += response.getRowsDeleted();
      }
    } catch (Throwable t) {
      throw new GdbException("Unable to delete search names in the specified channel type: "
          + channel, t);
    } finally {
      closeHTable(ht);
    }
    LOG.info("Number of affected HBase rows: " + numOfDeletedRows);
    return numOfDeletedRows;
  }

  @Deprecated
  private byte[] addNodeImpl(Node node) throws GdbException {
    byte[] nodeId = null;
    byte[] personIdBytes = null;
    HTableInterface hiValueTable = pool.getNodeNameTable(node.getType());
    HTableInterface hiIdTable = pool.getNodeIdTable(node.getType());
    HTableInterface hiAttrTable = pool.getNodeAttributeTable(node.getType());
    HTableInterface hiDetialTable = pool.getNodeWdeRefsTable(node.getType());
    String name = node.getName();
    try {
      while (true) {
        Get get =
            new Get(name.getBytes()).addColumn(NodeNameHTable.FAMILY, NodeNameHTable.QUALIFIER);
        nodeId = hiValueTable.get(get).getValue(NodeNameHTable.FAMILY, NodeNameHTable.QUALIFIER);
        if (nodeId != null) {// Node has been added
          personIdBytes = nodeId;
          break;
        } else {// Node needs to be added
          byte[] expectedId = checkforExpectedId(hiIdTable, node);
          personIdBytes = Bytes.add(node.getType().getBytesForm(), expectedId);
          if (!putValueIdPairWithCheck(hiValueTable, hiIdTable, personIdBytes, Bytes.toBytes(name))) {
            continue;// checkAndPut fail,so retry
          } else {// checkAndPut succeeded
            nodeId = personIdBytes;
          }
        }
      }
      // Add search names to HBase.
      if (!node.getSnames().isEmpty()) {
        List<String> newNamestoIndexed = new ArrayList<String>();
        while (true) {
          if (!putSearchNameWithCheck(hiIdTable, personIdBytes, node.getSnames(), newNamestoIndexed))
            continue;
          else {
            // Add search names to Index system.
            if (!newNamestoIndexed.isEmpty()) {
              if (false == addNodeToSearchIndex2(node, newNamestoIndexed, node.getName())) {
                throw new GdbException("Fail to addNodeToSearchIndex. Node Name=" + node.getName()
                    + " Node Type=" + node.getType());
              }
            }
            break;
          }
        }
      }

      node.setId(nodeId);// Node id is essential for Edge addition

      putWdeRefs(hiDetialTable, personIdBytes, node);
      updateAttributes(hiAttrTable, personIdBytes, node);
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(hiValueTable);
      closeHTable(hiIdTable);
      closeHTable(hiAttrTable);
      closeHTable(hiDetialTable);
    }
    return nodeId;
  }

  private String trimUnicodeHeader(String name) {
    byte[] nameBytes = Bytes.toBytes(name);
    if (nameBytes.length < 3) {
      return name;
    }
    if (nameBytes[0] == -17 // 0xef
        && nameBytes[1] == -69 // 0xbb
        && nameBytes[2] == -65) { // 0xbf
      byte[] out = new byte[nameBytes.length];
      System.arraycopy(nameBytes, 3, out, 0, nameBytes.length - 3);
      return Bytes.toString(out);
    } else
      return name;
  }
  
  
  public  void rebuildIndex(Channel channel) throws GdbException{
	    HTableInterface hiIdTable = pool.getNodeIdTable(NodeType.getType(channel, Attribute.PERSON));
	    Scan scan = new Scan();
	    Node node = null;
	    try {
			ResultScanner results = hiIdTable.getScanner(scan);
			for(Result result : results){
			    node = new Node(result.getRow());
				byte[] name = result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.QUALIFIER));
				if(name == null){
					continue;
				}
				String nodeName = Bytes.toString(name);
				node.setName(nodeName);
				byte[] sname = result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.SQUALIFIER));
				if(sname == null){
					continue;
				}
				byte[] addls = result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.AQUALIFIER));
				Quartet<String, NodeType, List<String>, String> tr =
		                new Quartet<String, NodeType, List<String>, String>(nodeName, node.getType(),
		                    getNewSnames(sname), getAdditional(addls)); 
				List<Quartet<String, NodeType, List<String>, String>>  trs = new ArrayList<Quartet<String, NodeType, List<String>, String>>();
				trs.add(tr);
				addNodeToSearchIndexBatched(trs);
			}
		} catch (IOException e) {
			 throw new GdbException("Fail to addNodeToSearchIndexBatched.  Node Name=" +node.getName() + " Node Type=" + node.getType() + " ");
		}
  }
  
/**
 * 逻辑调整,不再进行nodeId 判重.
 * 
 * */
  private byte[] addNodeImpl2(Node node, Boolean placeIsExists) throws GdbException {
    byte[] nodeId = null;
    byte[] personIdBytes = null;
    HTableInterface hiValueTable = pool.getNodeNameTable(node.getType());
    HTableInterface hiIdTable = pool.getNodeIdTable(node.getType());
    HTableInterface hiAttrTable = pool.getNodeAttributeTable(node.getType());
    HTableInterface hiDetialTable = pool.getNodeWdeRefsTable(node.getType());
    String name = node.getName();
    try {
      while (true) {
        Get get =
            new Get(name.getBytes()).addColumn(NodeNameHTable.FAMILY, NodeNameHTable.QUALIFIER);
       // nodeId = hiValueTable.get(get).getValue(NodeNameHTable.FAMILY, NodeNameHTable.QUALIFIER);
        if (hiValueTable.exists(get)) {// Node has been added
          personIdBytes = checkforExpectedId( node);
          placeIsExists = true;
          nodeId = personIdBytes;
          break;
        } else {// Node needs to be added
          personIdBytes = checkforExpectedId( node);// 不再在NodeId中判断是否重复.
          // personIdBytes = Bytes.add(node.getType().getBytesForm(), expectedId);
          if (!putValueIdPairWithCheck(hiValueTable, hiIdTable, personIdBytes, Bytes.toBytes(name))) {
            continue;// checkAndPut fail,so retry
          } else {// checkAndPut succeeded
            nodeId = personIdBytes;
          }
        }
      }
      
      // Add search names to HBase.
      if (!node.getSnames().isEmpty()) {
        List<String> newNamestoIndexed = new ArrayList<String>();
        // Get old search name from ID->Name table;
        byte[] oldSnames = getSearchName(hiIdTable, personIdBytes);
        // pack new search name into old search name.
        byte[] newSnames = packSearchName(oldSnames, node.getSnames(), newNamestoIndexed);
        // Add search names to Index system.
        // same as search names, but newAdditionaltoIndexed can not addTo index here! 
       
        List<String> newAdditionaltoIndexed = new ArrayList<String>();
        byte[] oldAdditionals = getAdditional(hiIdTable, personIdBytes);
        byte[] newAdditionals = packSearchName(oldAdditionals, node.getAdditionals(), newAdditionaltoIndexed);
        if(!newAdditionaltoIndexed.isEmpty()){
        	
        	batchedHbaseAdditionalCache.add(new Quartet<NodeType, byte[], List<String>, List<String>>(node
                .getType(), personIdBytes, node.getAdditionals(), newAdditionaltoIndexed));
            // add sname to hbase cache
        	
        	if(!newNamestoIndexed.isEmpty())
        		batchedHbaseSnameCache.add(new Quartet<NodeType, byte[], List<String>, List<String>>(node
                .getType(), personIdBytes, node.getSnames(), newNamestoIndexed));
        	
            Quartet<String, NodeType, List<String>, String> tr =
                new Quartet<String, NodeType, List<String>, String>(node.getName(), node.getType(),
                    getNewSnames(newSnames), getAdditional(newAdditionals)); 
            batchedNodeCache.add(tr);
        }
        else if (!newNamestoIndexed.isEmpty()) {
          // add sname to hbase cache
          batchedHbaseSnameCache.add(new Quartet<NodeType, byte[], List<String>, List<String>>(node
              .getType(), personIdBytes, node.getSnames(), newNamestoIndexed));
          
          Quartet<String, NodeType, List<String>, String> tr =
              new Quartet<String, NodeType, List<String>, String>(node.getName(), node.getType(),
                  newNamestoIndexed, getAdditional(newAdditionals)); 
          batchedNodeCache.add(tr);
        }
        if (batchedNodeCache.size() == NO_OF_BATCHED_NODES) {
            if (false == addNodeToSearchIndexBatched(batchedNodeCache)) {
              int cacheSize = batchedNodeCache.size();
              batchedNodeCache.clear();
              throw new GdbException("Fail to addNodeToSearchIndexBatched. Cache size=" + cacheSize
                  + " Node Name=" + node.getName() + " Node Type=" + node.getType() + " ");
            } else {
              batchedNodeCache.clear();
              // add sname from cache to hbase
              if (!addSnameToHbaseBatched(batchedHbaseSnameCache)) {
                batchedHbaseSnameCache.clear();
                throw new GdbException("Fail to addSnameToHbaseBatched.");
              }
              batchedHbaseSnameCache.clear();
              if (!addAdditionalToHbaseBatched(batchedHbaseAdditionalCache)) {
            	  batchedHbaseAdditionalCache.clear();
                  throw new GdbException("Fail to addAdditionalToHbaseBatched.");
                }
              batchedHbaseAdditionalCache.clear();
            }
          }
      }
      if(node.getEnts()!=null && !node.getEnts().isEmpty()){
    	  putEntsWithCheck(hiIdTable, nodeId, node.getEnts());
      }
      
      
      
      node.setId(nodeId);// Node id is essential for Edge addition
     
      putWdeRefs(hiDetialTable, personIdBytes, node);
      updateAttributes(hiAttrTable, personIdBytes, node);
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(hiValueTable);
      closeHTable(hiIdTable);
      closeHTable(hiAttrTable);
      closeHTable(hiDetialTable);
    }
    return nodeId;
  }

  /**
   * delete node from tables [NodeIdTable NodeNameTable NodeAttrTable NodeWdeRefsTable]
   * 
   * @param node
   * @throws GdbException
   */
  public void deleteNode(final Node node) throws GdbException {// node must contain its name and id
    assert node.getId() != null && node.getName() != null;
    final Scan scan = new Scan();
    scan.setStartRow(node.getId());
    PrefixFilter prefilter = new PrefixFilter(node.getId());
    FirstKeyOnlyFilter keyOnlyfilter = new FirstKeyOnlyFilter();
    FilterList filterlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    filterlist.addFilter(prefilter);
    filterlist.addFilter(keyOnlyfilter);
    scan.setFilter(filterlist);
    scan.setCaching(10000);
    String[] tableName = {"NodeIdTable", "NodeNameTable", "NodeAttributeTable", "NodeWdeRefsTable"};
    ArrayList<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
    try { // TODO do something to guarantee the delete is well done !
      results.add(exec.submit(new Callable<Boolean>() {
        public Boolean call() {
          return deleteNodeInNodeIdTable(node);
        }
      }));
      results.add(exec.submit(new Callable<Boolean>() {
        public Boolean call() {
          return deleteNodeInNodeNameTable(node);
        }
      }));
      results.add(exec.submit(new Callable<Boolean>() {
        public Boolean call() {
          return deleteNodeInNodeWdeRefsTable(node, scan);
        }
      }));
      results.add(exec.submit(new Callable<Boolean>() {
        public Boolean call() {
          return deleteNodeInNodeAttributeTable(node, scan);
        }
      }));
      String result = "";
      for (int i = 0; i < 4; i++) {
        if (!results.get(i).get()) result += tableName[i] + " ";
      }
      if (!result.equals(""))
        throw new GdbException("can't delete node from tables " + result
            + " after retries 10 times");
    } catch (Exception e) {
      throw new GdbException(e);
    }
  }

  private boolean deleteNodeInNodeIdTable(Node node) {
    HTableInterface hiIdTable = pool.getNodeIdTable(node.getType());
    Delete delete = new Delete(node.getId());
    int i = 0;
    try {
      for (; i < 10; i++) {
        // delete NodeIdTable
        Get get = new Get(node.getId());
        boolean exists = hiIdTable.exists(get);
        if (exists) { // just stop here? can reach the result that the node does not exist in whole
          if (i != 0) Thread.sleep(100);
          hiIdTable.delete(delete);
        } // cluster??
        else
          break;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      closeHTable(hiIdTable);
    }
    if (i == 10)
      return false;
    else
      return true;
  }

  private boolean deleteNodeInNodeNameTable(Node node) {
    HTableInterface hiNameTable = pool.getNodeNameTable(node.getType());
    Delete delete = new Delete(Bytes.toBytes(node.getName()));
    int i = 0;
    try {
      for (; i < 10; i++) {
        Get get = new Get(Bytes.toBytes(node.getName()));
        boolean exists = hiNameTable.exists(get);
        if (exists) {// just stop here? can reach the result that the node does not exist in whole
                     // cluster???
          if (i != 0) Thread.sleep(100);

          hiNameTable.delete(delete);
        } else
          break;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      closeHTable(hiNameTable);
    }
    if (i == 10)
      return false;
    else
      return true;
  }

  private boolean deleteNodeInNodeWdeRefsTable(Node node, Scan scan) {
    boolean success = false;
    HTableInterface hiAttrTable = pool.getNodeAttributeTable(node.getType());
    try {
      ResultScanner results = hiAttrTable.getScanner(scan);
      List<Delete> deleteAttrList = new ArrayList<Delete>();
      try {
        for (Result result : results)
          deleteAttrList.add(new Delete(result.getRow()));
      } finally {
        results.close();
      }
      for (int i = 0; i < 10; i++) {
        hiAttrTable.delete(deleteAttrList);// after this the list contains the delete not successful
        if (deleteAttrList.isEmpty()) {
          success = true;
          break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      closeHTable(hiAttrTable);
    }
    return success;
  }

  private boolean deleteNodeInNodeAttributeTable(Node node, Scan scan) {
    boolean success = false;
    HTableInterface hiDetailTable = pool.getNodeWdeRefsTable(node.getType());
    try {
      ResultScanner results = hiDetailTable.getScanner(scan);
      List<Delete> deleteAttrList = new ArrayList<Delete>();
      try {
        for (Result result : results)
          deleteAttrList.add(new Delete(result.getRow()));
      } finally {
        results.close();
      }
      for (int i = 0; i < 10; i++) {
        hiDetailTable.delete(deleteAttrList);// after this the list contains the delete not
                                             // successful
        if (deleteAttrList.isEmpty()) {
          success = true;
          break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      closeHTable(hiDetailTable);
    }
    return success;
  }

  private boolean addSnameToHbaseBatched(
      List<Quartet<NodeType, byte[], List<String>, List<String>>> batchedHbaseSnameCache)
      throws GdbException {
    boolean success = true;
    for (Quartet<NodeType, byte[], List<String>, List<String>> qr : batchedHbaseSnameCache) {
      HTableInterface hiIdTable = pool.getNodeIdTable(qr.getValue0());
      try {
        // should I add max retry times?
        final int max_try = 10;
        int t = 0;
        for (; t < max_try
            && !putSearchNameWithCheck(hiIdTable, qr.getValue1(), qr.getValue2(), qr.getValue3()); ++t) {
          ;
        }
        if (t == max_try) {
          success = false;
        }
      } catch (IOException e) {
        throw new GdbException(e);
      } finally {
        closeHTable(hiIdTable);
      }
    }
    return success;
  }
  private boolean addAdditionalToHbaseBatched(
		  List<Quartet<NodeType, byte[], List<String>, List<String>>> batchedHbaseAddtionalCache)
	      throws GdbException {
	    boolean success = true;
	    for (Quartet<NodeType, byte[], List<String>, List<String>> qr : batchedHbaseAddtionalCache) {
	      HTableInterface hiIdTable = pool.getNodeIdTable(qr.getValue0());
	      try {
	        // should I add max retry times?
	        final int max_try = 10;
	        int t = 0;
	        for (; t < max_try
	            && !putAdditionalWithCheck(hiIdTable, qr.getValue1(), qr.getValue2(), qr.getValue3()); ++t) {
	          ;
	        }
	        if (t == max_try) {
	          success = false;
	        }
	      } catch (IOException e) {
	        throw new GdbException(e);
	      } finally {
	        closeHTable(hiIdTable);
	      }
	    }
	    return success;
	  }
  private boolean putAdditionalWithCheck(HTableInterface hiIdTable, byte[] personIdBytes,
	      List<String> additionals, List<String> newAdditionalstoIndexed) throws IOException {
	    boolean successPutId = false;
	    Put putId = new Put(personIdBytes);
	    // Get old search name from ID->Name table;
	    byte[] oldAdditionals = getAdditional(hiIdTable, personIdBytes);
	    // pack new search name into old search name.
	    byte[] newAdditionals = packSearchName(oldAdditionals, additionals, newAdditionalstoIndexed);
	    if (newAdditionalstoIndexed.size() == 0) return true;
	    putId
	        .add(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.AQUALIFIER), newAdditionals);
	    successPutId =
	        hiIdTable.checkAndPut(personIdBytes, Bytes.toBytes(NodeIdHTable.FAMILY),
	            Bytes.toBytes(NodeIdHTable.AQUALIFIER), oldAdditionals, putId);
	    return successPutId;
	  }

  public void finishUpdate() throws GdbException {
    if (batchedNodeCache.size() == 0)
      return;
    else if (false == addNodeToSearchIndexBatched(batchedNodeCache)) {
      int cacheSize = batchedNodeCache.size();
      batchedNodeCache.clear();
      throw new GdbException("Fail to addNodeToSearchIndexBatched. Cache size=" + cacheSize);
    } else {
      batchedNodeCache.clear();
      // add sname from cache to hbase
      if (!addSnameToHbaseBatched(batchedHbaseSnameCache)) {
        batchedHbaseSnameCache.clear();
        throw new GdbException("Fail to addSnameToHbaseBatched.");
      }
      batchedHbaseSnameCache.clear();
      
      if (!addAdditionalToHbaseBatched(batchedHbaseAdditionalCache)) {
          batchedHbaseAdditionalCache.clear();
          throw new GdbException("Fail to addAdditionalToHbaseBatched.");
        }
      batchedHbaseAdditionalCache.clear();
    }
  }

  // /**
  // * Separator inside a given name.<br>
  // * For example:<br>
  // * in scholar: "Zhang San\u0001MSRA", "Li Si\u0001MIT"<br>
  // * in BBS: "myid\u0001qq.com"
  // */
  // private static final char NAME_INTERNAL_SEP = '\u0001';
  // @Deprecated
  // private boolean addNodeToSearchIndex(Node node) {
  // int idx = node.getName().indexOf(NAME_INTERNAL_SEP);
  // String indexedName = idx < 0 ? node.getName() :
  // node.getName().substring(0, idx);
  // return searchService.addIndex(indexedName, node.getName(),
  // node.getType());
  // }

  /**
   * Separator between sNames in HBase
   */
  private static final String SNAME_INTERNAL_SEP = "\u0001";

  private boolean addNodeToSearchIndex2(Node node, List<String> newNamestoIndexed, String name) {
    boolean success = false;
    Iterator<String> it = newNamestoIndexed.iterator();
    while (it.hasNext()) {
      String indexedName = it.next();
      success = searchService.addIndex(indexedName, name, "", node.getType(),(short)AdaModeConfig.getIndexNumber(node.getType().getChannel()));
      if (success == false) return success;
    }
    return success;
  }
  
  
  
  /**
   * Add node to index system in a batched mode.
   * 
   * @param raw raw data to be indexed, like: [{name1, type1, [sname1, sname2, ...,
   *        snamen],additional}, {name2, type2, [sname1, sname2, ..., snamen],additional}, ...,
   *        {namen, typen, [sname1, sname2, ..., snamen],additional}].
   * @return
   */
  private boolean addNodeToSearchIndexBatched(
      List<Quartet<String, NodeType, List<String>, String>> raw) {
    Map<NodeType, List<Triplet<String, String, String>>> typeToPairsMap =
        new HashMap<NodeType, List<Triplet<String, String, String>>>();
    // reorganize data according to NodeType
    for (Quartet<String, NodeType, List<String>, String> entry : raw) {
      String name = entry.getValue0();
      NodeType type = entry.getValue1();
      List<String> sNames = entry.getValue2();
      String additional = entry.getValue3();

      List<Triplet<String, String, String>> pairList = typeToPairsMap.get(type);
      if (pairList == null) {
        pairList = new ArrayList<Triplet<String, String, String>>();
      }
      for (String sName : sNames) {
        if (!sName.isEmpty()) {
          pairList.add(new Triplet<String, String, String>(sName, name, additional));// sName as
        }
        // Key, name
        // as Value
      }
      typeToPairsMap.put(type, pairList);
    }
    // start to add each type's data to search system
    boolean success = true;
    for (Entry<NodeType, List<Triplet<String, String, String>>> entry : typeToPairsMap.entrySet()) {
      NodeType type = entry.getKey();
      List<Triplet<String, String, String>> data = entry.getValue();
      final int RETYR_MAX = 2;// retry 2 times before giving up
      int retry = 0;
      boolean result = searchService.addIndexBatched2(data, type,(short)AdaModeConfig.getIndexNumber(type.getChannel()));
      while (false == result && retry < RETYR_MAX) {
        retry++;
        LOG.warn("start " + retry + "th retry after 5 secs...");
        try {
          Thread.sleep(5 * 1000);// sleep 5 seconds before retry
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        // retry
        result = searchService.addIndexBatched2(data, type,(short)AdaModeConfig.getIndexNumber(type.getChannel()));
      }
      if (result == false) {
        LOG.error("Failed to addToIndex though we tried. type=" + type + " data pair size="
            + data.size());
        success = false;
      }
    }
    return success;
  }

  private boolean putValueIdPairWithCheck(HTableInterface hiValueTable, HTableInterface hiIdTable,
      byte[] personIdBytes, byte[] name) throws IOException {
    boolean successPutValue = false;
    boolean successPutId = false;
    Put putValue = new Put(name);
    putValue.add(NodeNameHTable.FAMILY, NodeNameHTable.QUALIFIER, personIdBytes);
    successPutValue =
        hiValueTable.checkAndPut(name, NodeNameHTable.FAMILY, NodeNameHTable.QUALIFIER, null,
            putValue);// 如果空缺则插入
    if (!successPutValue) return false;

    Put putId = new Put(personIdBytes);
    putId.add(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.QUALIFIER), name);
    successPutId =
        hiIdTable.checkAndPut(personIdBytes, Bytes.toBytes(NodeIdHTable.FAMILY),
            Bytes.toBytes(NodeIdHTable.QUALIFIER), null, putId);
    return successPutId;
  }

  private boolean putSearchNameWithCheck(HTableInterface hiIdTable, byte[] personIdBytes,
      List<String> snames, List<String> newNamestoIndexed) throws IOException {
    boolean successPutId = false;
    Put putId = new Put(personIdBytes);
    // Get old search name from ID->Name table;
    byte[] oldSnames = getSearchName(hiIdTable, personIdBytes);
    // pack new search name into old search name.
    byte[] newSnames = packSearchName(oldSnames, snames, newNamestoIndexed);
    if (newNamestoIndexed.size() == 0) return true;
    putId
        .add(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.SQUALIFIER), newSnames);
    successPutId =
        hiIdTable.checkAndPut(personIdBytes, Bytes.toBytes(NodeIdHTable.FAMILY),
            Bytes.toBytes(NodeIdHTable.SQUALIFIER), oldSnames, putId);
    return successPutId;
  }

  private boolean putEntsWithCheck(HTableInterface hiIdTable, byte[] personIdBytes,
	      List<String> ents) throws IOException{
	    boolean successPutId = false;
	    Put putId = new Put(personIdBytes);
	    byte[] oldEnts = getEnts(hiIdTable, personIdBytes);
	    List<String> newEntList = new ArrayList<String>();
	    byte[] newEnts = packSearchName(oldEnts, ents, newEntList);
	    if (newEntList.size() == 0) return true;
	    putId
	        .add(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.EQUALIFIER), newEnts);
	    successPutId =
	        hiIdTable.checkAndPut(personIdBytes, Bytes.toBytes(NodeIdHTable.FAMILY),
	            Bytes.toBytes(NodeIdHTable.EQUALIFIER), oldEnts, putId);
	    return successPutId;
  }
  private String getAdditional(byte[] additionals){
	  String additionalsStr = Bytes.toString(additionals);
	  String[] additionalsArray = null;
	  if(additionalsStr != null){
		  additionalsArray = additionalsStr.split(SNAME_INTERNAL_SEP);
		  if(additionalsArray.length == 0) return "";
		  StringBuilder additionalIndex = new StringBuilder();
		  for(int i = 0; i <additionalsArray.length-1; i++) {
			  additionalIndex.append(additionalsArray[i]).append(",");
		  }
		  additionalIndex.append(additionalsArray[additionalsArray.length-1]);
		  return additionalIndex.toString();
	  }
	  return "";
	  
  }
  
  private List<String> getNewSnames(byte[] newSnames){
	  String snamesStr = Bytes.toString(newSnames);
	  String[] snamesArray = null;
	  List<String> snames = new ArrayList<String>();
	  if(snamesStr != null){
		  snamesArray = snamesStr.split(SNAME_INTERNAL_SEP);
		  for(String sname : snamesArray) {
			  snames.add(sname);
		  }
		  return snames;
	  }
	  return Collections.emptyList();
	  
  
  }
  
  private byte[] packSearchName(byte[] oldSnames, List<String> snames,
      List<String> newNamestoIndexed) {
    // if newNamestoIndexed is not empty, clear it.
    if (!newNamestoIndexed.isEmpty()) newNamestoIndexed.clear();
    String oldSnamesStr = Bytes.toString(oldSnames);
    String[] oldSnamesArray = null;
    LinkedHashSet<String> oldSnamesSet = new LinkedHashSet<String>();
    if (oldSnamesStr != null) {
      oldSnamesArray = oldSnamesStr.split(SNAME_INTERNAL_SEP);

      for (int i = 0; i < oldSnamesArray.length; i++) {
        if (!oldSnamesSet.contains(oldSnamesArray[i])) {// 没必要吧.
          oldSnamesSet.add(oldSnamesArray[i]);
        }
      }
    }

    Iterator<String> iter = snames.iterator();
    while (iter.hasNext()) {
      String sname = iter.next();
      if (!oldSnamesSet.contains(sname)) {
        oldSnamesSet.add(sname);
        newNamestoIndexed.add(sname);
      }
    }
    StringBuilder newSnames = new StringBuilder();
    Iterator<String> setIter = oldSnamesSet.iterator();
    
    while (setIter.hasNext()) {
      String sname = setIter.next();
      newSnames.append(sname).append(SNAME_INTERNAL_SEP);
    }
    return Bytes.toBytes(newSnames.toString());
  }

  private boolean putSingleAttributeWithCheck(HTableInterface htableInterface, byte[] attrRowkey,
      byte[] attrName, byte[] attrValue, byte[] oldAttrValue) throws IOException {
    Put putValue = new Put(attrRowkey);
    putValue.add(Bytes.toBytes(NodeAttributeHTable.FAMILY), attrName, attrValue);
    return htableInterface.checkAndPut(attrRowkey, Bytes.toBytes(NodeAttributeHTable.FAMILY),
        attrName, oldAttrValue, putValue);
  }

  private byte[] getAttrubiteByAttrName(HTableInterface htableInterface, byte[] attrRowkey,
      byte[] attrName) throws IOException {
    Get get = new Get(attrRowkey);
    get.addColumn(Bytes.toBytes(NodeAttributeHTable.FAMILY), attrName);
    Result result = htableInterface.get(get);
    if (result == null || result.isEmpty()) {
      return null;
    } else {
      return result.getValue(Bytes.toBytes(NodeAttributeHTable.FAMILY), attrName);
    }
  }

  public void updateEnts(HTableInterface htableInterface, byte[] personIdBytes, Node node){
	  
  }
  
  private void updateAttributes(HTableInterface htableInterface, byte[] personIdBytes, Node node)
      throws IOException {
    List<NodeAttribute> attributes = node.getAttributes();
    if (attributes == null || attributes.isEmpty()) return;
    for (int i = 0; i < attributes.size(); i++) {
      NodeAttribute attr = attributes.get(i);
      String key = attr.getKey();
      List<AttrValueInfo> listAttrValueInfo = attr.getValues();
      if (listAttrValueInfo.isEmpty()) continue;
      byte[] attrRowkey = Bytes.add(personIdBytes, key.getBytes());
      if (COMMUNITYNAME.contains(key)) { // 不是追加更新，而是替换。
        Get get = new Get(attrRowkey);
        if (htableInterface.exists(get)) {
          htableInterface.delete(new Delete(attrRowkey));
          htableInterface.flushCommits();
        }
        for (int j = 0; j < listAttrValueInfo.size(); j++) {
          while (true) {
            AttrValueInfo attrValueInfo = listAttrValueInfo.get(j);
            String value = attrValueInfo.getValue();
            List<WdeRef> listWderefs = attrValueInfo.getWdeRefs();
            byte[] attrName = Bytes.toBytes(value);
            SortedWdeRefSet set = new SortedWdeRefSet();
            set.add(listWderefs);
            if (putSingleAttributeWithCheck(htableInterface, attrRowkey, attrName, set.getBytes(),
                null)) {// if the column not exists put in.
              break;
            }
          }
        }
        continue;
      }         //action need to add TimeStamp in rowKey
			if (key.equals(ACTION) && AdaConfig.GRAPH_NODEATTR_ACTION_TIMESTAMP) {
				for (AttrValueInfo attrValueInfo : listAttrValueInfo) {
					byte[] attrName = Bytes.toBytes(attrValueInfo.getValue());
					Map<Integer, List<WdeRef>> wdeRefMap = groupAttrWdeRefList(attrValueInfo.getWdeRefs());
					for (Entry<Integer, List<WdeRef>> e : wdeRefMap.entrySet()) {
						byte[] attrRowkeyWithTs = Bytes.add(attrRowkey,NodeAttributeHTable.DELIMITER,Bytes.toBytes(e.getKey()));
						List<WdeRef> listWdeRefs = e.getValue();
						while (true) {
							byte[] attrOldWdeRef = getAttrubiteByAttrName(htableInterface, attrRowkeyWithTs, attrName);
							SortedWdeRefSet set = new SortedWdeRefSet(attrOldWdeRef);
							set.add(listWdeRefs);
							if (putSingleAttributeWithCheck(htableInterface,attrRowkeyWithTs, attrName, set.getBytes(),attrOldWdeRef)) {
								break;
							}
						}
					}
				}
			}
 else {
				for (AttrValueInfo attrValueInfo : listAttrValueInfo) {
					byte[] attrName = Bytes.toBytes(attrValueInfo.getValue());
					List<WdeRef> listWderefs = attrValueInfo.getWdeRefs();
					while (true) {
						byte[] attrOldWdeRef = getAttrubiteByAttrName(htableInterface, attrRowkey, attrName);
						SortedWdeRefSet set = new SortedWdeRefSet(attrOldWdeRef);
						set.add(listWderefs);
						if (putSingleAttributeWithCheck(htableInterface,attrRowkey, attrName, set.getBytes(),attrOldWdeRef)) {
							break;
						}
					}
				}
			}}
  }
  
  /**
   * Group the WdeRef List according to timestamp range.
   * 
   * @param refListgroupWdeRefList
   * @return a Map from "timestamp range start" to "WdeRefs in this timestamp range"
   */
  private Map<Integer, List<WdeRef>> groupAttrWdeRefList(List<WdeRef> refList) {
	  Map<Integer, List<WdeRef>> wdeRefMap = new HashMap<Integer, List<WdeRef>>();
	  if(refList==null||refList.size()==0){
		  wdeRefMap.put(0, Collections.<WdeRef> emptyList());
		  return wdeRefMap;
	  }
   
    for (WdeRef ref : refList) {
      int tsRangeStart = getTsRangeStartAction(ref.getTimestamp());
      List<WdeRef> list = wdeRefMap.get(tsRangeStart);
      if (list == null) {
        list = new ArrayList<WdeRef>();
        wdeRefMap.put(tsRangeStart, list);
      }
      list.add(ref);
    }
    return wdeRefMap;
  }
  private boolean putSingleWdeRefWithCheck(HTableInterface htableInterface, byte[] wderefRowkey,
      byte[] newWdeRef, byte[] oldWdeRefId) throws IOException {
    Put putValue = new Put(wderefRowkey);
    putValue.add(Bytes.toBytes(NodeWdeRefsHTable.FAMILY),
        Bytes.toBytes(NodeWdeRefsHTable.QUALIFIER), newWdeRef);
    return htableInterface.checkAndPut(wderefRowkey, Bytes.toBytes(NodeWdeRefsHTable.FAMILY),
        Bytes.toBytes(NodeWdeRefsHTable.QUALIFIER), oldWdeRefId, putValue);
  }

  private void putWdeRefs(HTableInterface htableInterface, byte[] personIdBytes, Node node)
      throws IOException {
    List<WdeRef> wdeRefs = node.getWdeRefs();
    if (wdeRefs == null || wdeRefs.size() == 0) return;
    Map<Integer, List<WdeRef>> mapWdeRef = new HashMap<Integer, List<WdeRef>>();
    for (WdeRef wdeRef : wdeRefs) {
      int timestamp = wdeRef.getTimestamp();
      int date = getTsRangeStart(timestamp);
      List<WdeRef> listWdeRef = mapWdeRef.get(date);
      if (listWdeRef == null) {
        listWdeRef = new LinkedList<WdeRef>();
        mapWdeRef.put(date, listWdeRef);
      }
      listWdeRef.add(wdeRef);
    }

    // Iterator<Entry<Integer, List<WdeRef>>> iter = mapWdeRef.entrySet().iterator();
    for (Entry<Integer, List<WdeRef>> entry : mapWdeRef.entrySet()) {
      // Map.Entry<Integer, List<WdeRef>> entry = iter.next();
      int date = entry.getKey();
      List<WdeRef> val = entry.getValue();
      byte[] wderefRowkey = Bytes.add(personIdBytes, Bytes.toBytes(date));
      boolean success = false;
      while (!success) {
        byte[] oldWdeRefs = getWdeRefRowkey(htableInterface, wderefRowkey);
        SortedWdeRefSet set = new SortedWdeRefSet(oldWdeRefs);
        set.add(val);
        if (!putSingleWdeRefWithCheck(htableInterface, wderefRowkey, set.getBytes(), oldWdeRefs)) {
          continue;
        }
        success = true;
      }
    }
  }

  private boolean nodeExists(HTableInterface htableInterface, Node node) throws IOException {
    Get get = new Get(Bytes.toBytes(node.getName()));
    return htableInterface.exists(get);
  }

  private byte[] getWdeRefRowkey(HTableInterface htableInterface, byte[] wderefRowkey)
      throws IOException {
    Get get = new Get(wderefRowkey);
    get.addColumn(Bytes.toBytes(NodeWdeRefsHTable.FAMILY),
        Bytes.toBytes(NodeWdeRefsHTable.QUALIFIER));
    Result result = htableInterface.get(get);
    return result.getValue(Bytes.toBytes(NodeWdeRefsHTable.FAMILY),
        Bytes.toBytes(NodeWdeRefsHTable.QUALIFIER));
  }

  private byte[] getSearchName(HTableInterface htableInterface, byte[] personId) throws IOException {
    Get get = new Get(personId);
    get.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.SQUALIFIER));
    Result result = htableInterface.get(get);
    return result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY),
        Bytes.toBytes(NodeIdHTable.SQUALIFIER));
  }

  private byte[] getAdditional(HTableInterface htableInterface, byte[] personId)throws IOException{
	    Get get = new Get(personId);
	    get.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.AQUALIFIER));
	    Result result = htableInterface.get(get);
	    return result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY),
	        Bytes.toBytes(NodeIdHTable.AQUALIFIER));
  }
  
  private byte[] getEnts(HTableInterface htableInterface, byte[] nodeId) throws IOException{
	    Get get = new Get(nodeId);
	    get.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(NodeIdHTable.EQUALIFIER));
	    Result result = htableInterface.get(get);
	    return result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY),
	        Bytes.toBytes(NodeIdHTable.EQUALIFIER));
  }
  private byte[] checkforExpectedId(HTableInterface htableInterface, Node node) throws IOException {
    byte[] expectedId = null;
    String nodeName = node.getName().substring(4);// 在算md5时不包括前缀.
    byte[] personId = Bytes.add(node.getType().getBytesForm(), DigestUtils.md5(nodeName)); // 完整的rowId
                                                                                           // 应包括前缀
    while (true) {
      Get get = new Get(personId);
      if (htableInterface.exists(get)) { // MD5 collision in gdb, re-MD5
        // personId.
        personId = Bytes.add(node.getType().getBytesForm(), DigestUtils.md5(personId));
      } else {
        expectedId = personId;
        break;
      }
    }
    return expectedId;
  }

  private byte[] checkforExpectedId(Node node){

	    String nodeName = node.getName().substring(4);// 在算md5时不包括前缀.
	    byte[] expectedId = Bytes.add(node.getType().getBytesForm(), DigestUtils.md5(nodeName)); // 完整的rowId
	                                                                                           // 应包括前缀
	    return expectedId;
	  
  }
  private int getTsRangeStart(int ts) {
    return ts <= 0 ? 0 : ts - ts % GdbHTableConstant.TIME_GRANULARITY;
  }
  private int getTsRangeStartAction(int ts) {
	    return ts <= 0 ? 0 : ts - ts % GdbHTableConstant.ACTION_TIME_GRANULARITY;
	  }
  private Node getNodeAttributesByNameAndType(HTableInterface hiAttrTable, Node node,boolean loadInfo)
      throws GdbException {
    // Node node = new Node(type, name);
    byte[] id = getNodeIdByName(node.getName(), node.getType());
    node.setId(id);
    return getNodeAttributesById(hiAttrTable, node,loadInfo);
  }
  private Node getNodeAttributesById(HTableInterface hiAttrTable, Node node) throws GdbException {
	    //
	    byte[] id = node.getId();
	    // System.arraycopy(id, 0, type, 0, 2);
	    // Node node = new Node(id);
	    Scan scan = new Scan();
	    // Filter filter = new PrefixFilter(id);
	    // scan.setFilter(filter);
	    scan.setStartRow(id);//
	    byte[] stoprow = new byte[id.length];
	    System.arraycopy(id, 0, stoprow, 0, id.length);
	    for (int i = id.length - 1;; i--) { // TODO byte array plus one should put
	                                        // to a util class and gen a static
	                                        // method
	      stoprow[i]++;
	      if (stoprow[i] != 0) break;
	    }
	    scan.setStopRow(stoprow);
	    // byte[] stoprow={(byte)(0xff),(byte)(0xff)};
	    // scan.setStopRow(Bytes.add(id, stoprow));
	    ResultScanner scanner = null;
	    try {
	      scanner = hiAttrTable.getScanner(scan);
	      for (Result res : scanner) {
	        byte[] row = res.getRow();
	        List<AttrValueInfo> attrValInfos = new LinkedList<NodeAttribute.AttrValueInfo>();
	        byte[] attrName = Bytes.tail(row, row.length - id.length);
	        if(isActionAttribute(attrName))//action do not belong to attributes.
	        	continue;
	        for (KeyValue kv : res.raw()) {
	          byte[] attrValue = kv.getQualifier();
	          // byte[] attrName = Bytes.tail(row, row.length - id.length);
	          byte[] wdeIds = kv.getValue();
	          List<WdeRef> wdeRefs = (new SortedWdeRefSet(wdeIds)).getList();
	          // List<AttrValueInfo> attrValInfos = new
	          // LinkedList<NodeAttribute.AttrValueInfo>();
	          attrValInfos.add(new AttrValueInfo(Bytes.toString(attrValue), wdeRefs));
	        }
	        NodeAttribute nodeAttr = new NodeAttribute(Bytes.toString(attrName), attrValInfos);
	        node.addNodeAttribute(nodeAttr);
	      }
	    } catch (IOException e) {
	      throw new GdbException(e);
	    } finally {
	      scanner.close();
	      closeHTable(hiAttrTable);
	    }

	    return node;
	  
  }
  //TODO 会导致接口失效
  public byte[] getNodeComputeValue(byte[] id, String computation) throws GdbException {
	  Node node = new Node(id);
	  HTableInterface hiIdTable = pool.getNodeIdTable(node.getType());
	  try {
	  Get get = new Get(id);
      get.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(computation));
      Result result = hiIdTable.get(get);
      byte[] val = result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(computation));
      return val;
	} catch (IOException e) {
		  throw new GdbException(e);
	}finally{
		closeHTable(hiIdTable);
	}
      
  }
  
  private Node getNodeAttributesById(HTableInterface hiAttrTable, Node node,boolean loadInfo) throws GdbException {
    if(loadInfo)return getNodeAttributesById(hiAttrTable,node);
    byte[] id = node.getId();
    // System.arraycopy(id, 0, type, 0, 2);
    // Node node = new Node(id);
    Scan scan = new Scan();
    // Filter filter = new PrefixFilter(id);
    // scan.setFilter(filter);
    scan.setStartRow(id);//
    byte[] stoprow = new byte[id.length];
    System.arraycopy(id, 0, stoprow, 0, id.length);
    for (int i = id.length - 1;; i--) { // TODO byte array plus one should put
                                        // to a util class and gen a static
                                        // method
      stoprow[i]++;
      if (stoprow[i] != 0) break;
    }
    scan.setStopRow(stoprow);
    KeyOnlyFilter filter =new KeyOnlyFilter(true);
    scan.setFilter(filter);
    // byte[] stoprow={(byte)(0xff),(byte)(0xff)};
    // scan.setStopRow(Bytes.add(id, stoprow));
    ResultScanner scanner = null;
    try {
      scanner = hiAttrTable.getScanner(scan);
      for (Result res : scanner) {
        byte[] row = res.getRow();
        List<AttrValueInfo> attrValInfos = new LinkedList<NodeAttribute.AttrValueInfo>();
        byte[] attrName = Bytes.tail(row, row.length - id.length);
        if(isActionAttribute(attrName)){//action do not belong to attributes.
        	continue;
        	}
        for (KeyValue kv : res.raw()) {
          byte[] attrValue = kv.getQualifier();
          int weight=Bytes.toInt(kv.getValue())/SortedWdeRefSet.WDEREF_SIZE;
          // byte[] attrName = Bytes.tail(row, row.length - id.length);
         // byte[] wdeIds = kv.getValue();
          //List<WdeRef> wdeRefs = (new SortedWdeRefSet(wdeIds)).getList();
          // List<AttrValueInfo> attrValInfos = new
          // LinkedList<NodeAttribute.AttrValueInfo>();
          attrValInfos.add(new AttrValueInfo(Bytes.toString(attrValue), weight));
        }
        NodeAttribute nodeAttr = new NodeAttribute(Bytes.toString(attrName), attrValInfos);
        node.addNodeAttribute(nodeAttr);
      }
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      scanner.close();
      closeHTable(hiAttrTable);
    }

    return node;
  }
private boolean isActionAttribute(byte[] attrName){
	byte[] action=Bytes.add(ACTION.getBytes(), NodeAttributeHTable.DELIMITER);
	return attrName.length==action.length+4&&Bytes.equals(attrName, 0,action.length,action,0,action.length);
}
  /**
   * @param id 通过id[0] id[1] 获取type
   * @return
   */
  private NodeType getNodeTypeFromId(byte[] id) {

    NodeType nodetype = NodeType.getType(id[0], id[1]);
    return nodetype;
  }

  private void addLocationNodeTask(Node node) throws GdbException {
    String name = node.getName();
    int current = (int) (System.currentTimeMillis() / 1000);
    String rowKey = String.valueOf(current) + name;
    Put putValue = new Put(rowKey.getBytes());
    putValue.add(Bytes.toBytes(LocationNodeTasksHTable.FAMILY),
        Bytes.toBytes(LocationNodeTasksHTable.QUALIFIER), Bytes.toBytes(false));
    HTableInterface hiLocationTable = pool.getLocationNodeTaskTable();
    try {
      hiLocationTable.put(putValue);
    } catch (IOException e) {
      throw new GdbException(e);
    } finally {
      closeHTable(hiLocationTable);
    }
  }

  /**
   * @param node <br>
   *        the node to query
   * @param method <br>
   *        the community method =the key of value
   * @return<br> the node may in many communities.Nodes have no names.
   * @throws GdbException
   */
  public List<List<Node>> getNodeCommunityPersonRelList(Node node, String method)
      throws GdbException {
    if (!COMMUNITYNAME.contains(method))
      throw new GdbException("the method:" + method + " not suported!");
    List<List<Node>> relNodeList = new ArrayList<List<Node>>();// always in a single channel?
    HTableInterface hiAttrTable = pool.getNodeAttributeTable(node.getType());
    byte[] attrRowkey = Bytes.add(node.getId(), method.getBytes());
    String personClusters = "";
    try {
      Get get = new Get(attrRowkey);
      Result result = null;
      try {
        result = hiAttrTable.get(get);
      } catch (IOException e) {
        throw new GdbException(e);
      }

      if (!result.isEmpty()) {
        // if has,always only has one column
        personClusters = Bytes.toString(result.raw()[0].getQualifier());
      }
    } finally {
      closeHTable(hiAttrTable);
    }
    if (personClusters.equals(""))
      return relNodeList;
    else {
      String[] personClusterA = personClusters.split("[|]");
      for (String personCluster : personClusterA) {
        List<Node> nodes =
            getNodeListByCommunityNumber(Integer.parseInt(personCluster), node.getType(), method);
        relNodeList.add((nodes));
      }
      return relNodeList;
    }

  }

  /**
   * @param personClusterNum
   * @param nodeType
   * @param method
   * @return
   * @throws GdbException
   */
  private List<Node> getNodeListByCommunityNumber(int personClusterNum, NodeType nodeType,
      String method) throws GdbException {
    // row id = nodeType byte form + method + personClusterNum byte form.

    byte[] personClusterId =
        Bytes.add(nodeType.getBytesForm(), method.getBytes(), Bytes.toBytes(personClusterNum));
    HTableInterface communityPersonReltable = pool.getCommunityPersonRelTable();
    byte[] nodeIds = null;
    try {
      Get get = new Get(personClusterId);
      Result result = null;
      try {
        result = communityPersonReltable.get(get);
      } catch (IOException e) {
        throw new GdbException(e);
      }
      // nodeids is the md5 list
      nodeIds =
          result.getValue(CommunityPersonRelHTable.FAMILY.getBytes(),
              CommunityPersonRelHTable.QUALIFIER.getBytes());
    } finally {
      closeHTable(communityPersonReltable);
    }
    List<Node> nodes = new ArrayList<Node>();
    if (nodeIds == null) return nodes;
    for (int i = 0; i < nodeIds.length; i += 16) {
      nodes
          .add(new Node(Bytes.add(nodeType.getBytesForm(), Arrays.copyOfRange(nodeIds, i, i + 16))));
    }
    return nodes;
  }
  public long cleanNodeHTable(Channel channel){
		HTableInterface nodeIdTable=pool.getNodeIdTable(NodeType.getType(channel, Attribute.PERSON));
		 Map<byte[], Long> results=null;
		 long count=0;
		 try{
		 long st = Timer.now();
			results = nodeIdTable.coprocessorExec(HBaseNodeDaoProtocol.class, null,
			         null,
			         new Batch.Call<HBaseNodeDaoProtocol, Long>() {
			           public Long call(HBaseNodeDaoProtocol instance) { 	    
							try {
								return instance.cleanNodeHTable();
							} catch (Exception e) {
								
								e.printStackTrace();
							}
							return null;	          
			           }
			         });
		   
		   for(Map.Entry<byte[], Long>e:results.entrySet()){
			   count+=e.getValue();
		   }
	       LOG.info(Bytes.toString(nodeIdTable.getTableName()) + ":clean complete in  "
	           + Timer.msSince(st) + "ms"+", the number of row delete :"+count+". " );
		 }catch(Throwable e){
			 e.printStackTrace();
		 }
		 return count;
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
}
