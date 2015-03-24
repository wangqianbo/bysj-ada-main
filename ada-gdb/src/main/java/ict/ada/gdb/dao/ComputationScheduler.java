package ict.ada.gdb.dao;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.schema.EdgeIdHTable;
import ict.ada.gdb.schema.EdgeRelWeightSumHTable;
import ict.ada.gdb.schema.NodeIdHTable;
import ict.ada.gdb.schema.NodeTaskHTable;
import ict.ada.gdb.util.ValueDecoderUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.javatuples.Pair;


public class ComputationScheduler {
	private static final byte[] EMPTY_VALUE = new byte[0];
	  private static final byte[] EMPTY_QUALIFIER = new byte[0];
	  private static final String COMPUTATION = "TR";
	  private static final float INITAIL_VALUE = 1.0f;
	private GdbHTablePool pool;

	  public ComputationScheduler(GdbHTablePool pool) {
	    this.pool = pool;
	  }
	  
	  public boolean schedule(long ts,Channel channel) throws IOException, GdbException{
		  HTableInterface edgeMemTable = pool.getEdgeMemTable(channel);
		  HTableInterface edgeIdTable = pool.getEdgeIdTable(NodeType.getType(channel, Attribute.ACCOUNT));
		  HTableInterface nodeIdTable = pool.getNodeIdTable(NodeType.getType(channel, Attribute.ACCOUNT));
		  Scan scan = new Scan();
		  scan.setMaxVersions();
		  scan.setTimeRange(0,ts);
		  ResultScanner rs = edgeMemTable.getScanner(scan);
		  List<Delete> deletes = new ArrayList<Delete>();
		  Iterator<Result> iter = rs.iterator();
		  byte[] nodeId = null;
		  List<byte[]> preAdjList = null;
		  while(iter.hasNext()){
			  Result result = iter.next();
			  if(nodeId ==null ){
				  nodeId = Bytes.head(result.getRow(), Node.NODEID_SIZE);
				  preAdjList = getNodeAdj(nodeId,edgeIdTable);
			  }else if(!Bytes.equals(nodeId, 0, Node.NODEID_SIZE, result.getRow(), 0, Node.NODEID_SIZE)){
				  byte[] val = getNodeComputeValue(nodeIdTable,nodeId,"TR");
				  if(val == null || preAdjList == null || preAdjList.size() == 0){
					  //TODO 直接把该点加入初始化表里，点会有初始值的，而且初始值应该在变化量上
					  addNodeChange(nodeId,INITAIL_VALUE);
				  }else{
					  Pair<Float,Float> value = ValueDecoderUtil.decode(val);
					  List<byte[]> adjList =  getNodeAdj(nodeId,edgeIdTable);
					  List<Pair<byte[],Float>> changeNodes = handle(preAdjList,adjList,value.getValue0());
					  //TODO 加入计算初始表
					  addNodeChange(changeNodes);
 				  }
				  nodeId = Bytes.head(result.getRow(), Node.NODEID_SIZE);
				  preAdjList = getNodeAdj(nodeId,edgeIdTable);
				  }
			  byte[] edgeId = result.getRow();
//			  Edge edge = new Edge();
			  NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> resMap = result.getMap();
			  deletes.add(new Delete(result.getRow(),ts-1));
			  addEdge(edgeId,resMap,NodeType.getType(channel, Attribute.ACCOUNT));
		  	}
		  if(nodeId != null){
			  byte[] val = getNodeComputeValue(nodeIdTable,nodeId,"TR");
			  if(val == null || preAdjList == null || preAdjList.size() == 0){
				  //TODO 直接把该点加入初始化表里，点会有初始值的，而且初始值应该在变化量上
				  addNodeChange(nodeId,INITAIL_VALUE);
			  }else{
				  Pair<Float,Float> value = ValueDecoderUtil.decode(val);
				  List<byte[]> adjList =  getNodeAdj(nodeId,edgeIdTable);
				  List<Pair<byte[],Float>> changeNodes = handle(preAdjList,adjList,value.getValue0());
				  //TODO 加入计算初始表
				  addNodeChange(changeNodes);
				  }
		  }
		  edgeMemTable.delete(deletes);
		  return true;
		  }
	  
	  private List<Pair<byte[],Float>> handle(List<byte[]> preAdjList, List<byte[]> adjList, float vertexValue){
		  List<Pair<byte[],Float>> result  = new ArrayList<Pair<byte[],Float>>();
		  if(preAdjList == null || preAdjList.size() == 0){
			  float change2 = vertexValue/adjList.size();
			  for(byte[] nodeId : adjList){
				  result.add(new Pair<byte[],Float>(nodeId,change2));
			  }
		  }else{
			  Iterator<byte[]> preAdjIter = preAdjList.iterator();
			  Iterator<byte[]> adjIter = adjList.iterator();
			  float change1 = vertexValue/preAdjList.size();
			  float change2 = vertexValue/adjList.size();
			  byte[] oldNode = preAdjIter.next();
			  byte[] newNode = adjIter.next();
			  while(true){
				  int flag = Bytes.compareTo(oldNode, newNode);
				  if(flag == 0){
					  if(change2 - change1 != 0.0f){
						  result.add(new Pair<byte[],Float>(newNode,change2-change1));
					  }
					  if(!preAdjIter.hasNext() || adjIter.hasNext())  { 
						  newNode = null;
						  break;
						  }
					  oldNode = preAdjIter.next();
					  newNode = adjIter.next();
				  }else if(flag > 0){
					  result.add(new Pair<byte[],Float>(newNode,change2));
					  if(!adjIter.hasNext()) { 
						  newNode = null;
						  break; }
					  newNode = adjIter.next();
				  }else{
					  if(!preAdjIter.hasNext()) { break; }
					  oldNode = preAdjIter.next();
				  }
			  }
			  if(newNode != null){
				  result.add(new Pair<byte[],Float>(newNode,change2));
			  }
			  while(adjIter.hasNext()){
				  result.add(new Pair<byte[],Float>(adjIter.next(),change2));
			  }
		  }
		 return result;
	  }
	  
	  private List<byte[]> getNodeAdj(byte[] nodeId,HTableInterface table) throws IOException{
		  Node node = new Node(nodeId);
		  byte[] scanPrefix = Bytes.add(nodeId,node.getType().getAttribute().getByteFrom());
		  PrefixFilter filter = new PrefixFilter(scanPrefix);
		  Scan scan = new Scan();
		  scan.setStartRow(scanPrefix);
		  scan.setFilter(filter);
		  scan.setCaching(1000);// 这个应该还是挺重要的。
		  List<byte[]> nodeIds = new ArrayList<byte[]>();
		    try {
		      ResultScanner results = table.getScanner(scan);
		      try {
		        for (Result result : results) {
		        	nodeIds.add(Bytes.tail(result.getRow(), Node.NODEID_SIZE));
		        }
		      } finally {
		        results.close();
		      }
		      return nodeIds;
		    } finally {
		      closeHTable(table);
		    }
		  	  }
	  
	  
	//TODO 会导致接口失效
	  public byte[] getNodeComputeValue(HTableInterface hiIdTable ,byte[] id, String computation) throws GdbException {
		  try {
		  Get get = new Get(id);
	      get.addColumn(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(computation));
	      Result result = hiIdTable.get(get);
	      byte[] val = result.getValue(Bytes.toBytes(NodeIdHTable.FAMILY), Bytes.toBytes(computation));
	      return val;
		} catch (IOException e) {
			  throw new GdbException(e);
		}
		  }
	  private void addEdge(byte[] edgeId, NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> result,NodeType nodeType) throws IOException{
		  HTableInterface edgeIdTable = pool.getEdgeIdTable(nodeType);
		  HTableInterface edgeWeightSumTable = pool.getEdgeRelWeightSumTable(nodeType);
		  if (!edgeIdTable.exists(new Get(edgeId))) {
		        Put put = new Put(edgeId);// edge id as row key
		        put.add(EdgeIdHTable.FAMILY, EMPTY_QUALIFIER, EMPTY_VALUE);
		        edgeIdTable.put(put);
		      }
		  for(Map.Entry<byte[], NavigableMap<Long,byte[]>> e:result.firstEntry().getValue().entrySet()){
			  byte[] relTypeCol =e.getKey();
			  int relWeight = 0;
			  for(byte[] v : e.getValue().values()){
				  relWeight += Bytes.toInt(v);
			  }
			  edgeWeightSumTable.incrementColumnValue(HBaseEdgeDAO.getSaltedRowKey(edgeId),
			          EdgeRelWeightSumHTable.FAMILY, relTypeCol, relWeight);
		  }
		    
	  }
	  
	  private  void addNodeChange(byte[] nodeId,float change) throws IOException{
		  Node node = new Node(nodeId);
		  HTableInterface nodeChangeTable = pool.getNodeTaskTable(node.getType());
		  Put put = new Put(nodeId);
		  put.add(NodeTaskHTable.FAMILY, Bytes.toBytes(COMPUTATION), Bytes.toBytes(change));
		  nodeChangeTable.put(put);
	  }
	  private void addNodeChange(List<Pair<byte[],Float>> changes) throws IOException{
		  NodeType nodeType = null;
		  List<Put> puts = new ArrayList<Put>();
		  for(Pair<byte[],Float> change : changes){
			  if(nodeType == null){
				  Node node = new Node(change.getValue0());
				  nodeType = node.getType();
			  }
			  Put put = new Put(change.getValue0());
			  put.add(NodeTaskHTable.FAMILY, Bytes.toBytes(COMPUTATION), Bytes.toBytes(change.getValue1()));
			  puts.add(put);
		  }
		  HTableInterface nodeChangeTable = pool.getNodeTaskTable(nodeType);
		  nodeChangeTable.put(puts);
	  }
	  
	  /**
	   * Close an HTable and log the Exception if any.
	   */
	  private void closeHTable(HTableInterface htable) {
	    if (htable == null) return;
	    try {
	      htable.close();
	    } catch (IOException e) {
//	      LOG.error("Fail to close HTable: " + Bytes.toString(htable.getTableName()), e);
	    }
	  }
}
