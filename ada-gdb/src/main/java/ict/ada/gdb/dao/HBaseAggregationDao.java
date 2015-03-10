package ict.ada.gdb.dao;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.AdaConfig;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.rowcounter.TableRowCount;
import ict.ada.gdb.schema.EdgeIdHTable;
import ict.ada.gdb.schema.NodeIdHTable;
import ict.ada.gdb.schema.TableRowCountTable;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * GDB's statistical information
 * 
 * */
public class HBaseAggregationDao {
  private final ExecutorService exec;

  private GdbHTablePool pool;
  private ObjectMapper m;
  private AggregationClient aggregationClient;
  private List<Channel> channels;
  public HBaseAggregationDao(GdbHTablePool pool) {
    exec = Executors.newCachedThreadPool();
    this.pool = pool;
    m = new ObjectMapper();
    Configuration customConf = new Configuration();
    customConf.setLong("hbase.rpc.timeout", 6000000);
    customConf.setLong("hbase.client.scanner.caching", 1000);
    Configuration configuration = HBaseConfiguration.create(customConf);
    aggregationClient = new AggregationClient(configuration);
    String channelString=AdaConfig.ROWCOUNT_CHANNELS;
    channels= new ArrayList<Channel>();
    if(channelString!=null)
       for(String ch:channelString.split(":"))
    	   channels.add(Channel.getChannel(Integer.parseInt(ch)));
    else  {
    	for(Channel channel: Channel.values())
    		channels.add(channel);
    }
    
  }


  private TableRowCount getAllTableRowCount1()  {//throws GdbException
	  TableRowCount bean = new TableRowCount(); 

    for (Channel channel : channels) {
      if (channel!=Channel.ANY) {
    	  Map<Integer,Long> nodeCount=getChannelNodeRowCount(channel);
    	  long edgeCount=getChannelEdgeRowCount(channel);
    	  if(nodeCount.size()!=0||edgeCount!=0){
    		  bean.addChannel(channel.getIntForm(), nodeCount, edgeCount);
    	  }
      }
    }
    return bean;
  }
  
  
  private Map<Integer,Long> getChannelNodeRowCount(Channel channel){
	  String nodeIdName=NodeIdHTable.getName(channel);
	  Map<Integer,Long> result= new HashMap<Integer,Long>();
	  if(pool.exist(nodeIdName)){
		  for(Attribute type: Attribute.values()){
			  if(type==Attribute.ANY)
				  continue;
			 Scan scan = new Scan();
			 scan.addFamily(NodeIdHTable.FAMILY.getBytes());
			 byte[] prefix = NodeType.getType(channel, type).getBytesForm(); 
			 scan.setStartRow(prefix);
			 List<Filter> filters=new ArrayList<Filter>();
			 filters.add(new PrefixFilter(prefix));
			 filters.add(new FirstKeyOnlyFilter());
			 FilterList filter= new FilterList(filters);
			 scan.setFilter(filter);
			 long count= getResult(nodeIdName,scan);
			 if(count>0)result.put(type.getIntForm(), count);
		  }
		  
	  }
	  return result;
  }

  private Long getChannelEdgeRowCount(Channel channel){
	  String edgeIdName = EdgeIdHTable.getName(channel);
	  long result=0L;
	  if(pool.exist(edgeIdName)){
		  Scan scan = new Scan();
		  scan.addFamily(EdgeIdHTable.FAMILY);
		  scan.setFilter(new FirstKeyOnlyFilter());
		  result = getResult(edgeIdName,scan);
	  }
	  return result;
  }
  private long getResult(String tableName, Scan scan)  {//throws GdbException
    long result = 0L;
    try {
      result = aggregationClient.rowCount(Bytes.toBytes(tableName), new LongColumnInterpreter(),
          scan);
    } catch (Throwable e) {
    	e.printStackTrace();
       // if table does not register coprocessor.AggregateImplementation,ignore it!
      if ((e instanceof org.apache.hadoop.hbase.ipc.HBaseRPC.UnknownProtocolException)) 
      result = 0L;// set 0 or -1???
    //  else throw new GdbException("Fail to get all Table rowCount" + e.getMessage() + e, e);
    }
    return result;

  }

  private synchronized void put(Map<String, Long> map, String key, long value) {
    map.put(key, value);
  }

  /**
   * count all the table's rowcount and store it
   * */
  public void addTableRowCount() throws GdbException {
  
    TableRowCount bean = getAllTableRowCount1();
   
    addTableRowCount(bean);
  }

  /**
   * get the newest record of all the table's rowCount.
   * */
  public TableRowCount getTableRowCount() throws GdbException {
	  TableRowCount rowCountBean = null;
    Scan scan = new Scan();
    scan.setBatch(1);
    scan.setCaching(1);
    scan.addColumn(TableRowCountTable.FAMILY, TableRowCountTable.QUALIFIER);
    HTableInterface rowCountTable = pool.getTableRowCountTable();
    try {
      ResultScanner scanner = rowCountTable.getScanner(scan);
      Result result = null;
      result=scanner.next();
      if (result == null) new GdbException("Fail to get tableRowCount.");
       String json= Bytes.toString(result.getValue(TableRowCountTable.FAMILY, TableRowCountTable.QUALIFIER));
       System.out.println(json);
      rowCountBean = m.readValue(
          Bytes.toString(result.getValue(TableRowCountTable.FAMILY, TableRowCountTable.QUALIFIER)),
          TableRowCount.class);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      throw new GdbException("Fail to get tableRowCount." + e);
    }
    return rowCountBean;
  }

  private void addTableRowCount(TableRowCount rowCount) throws GdbException {
    String jsonValue = null;
    StringWriter sw = new StringWriter();
    JsonFactory jf = new JsonFactory();
    JsonGenerator jg;
    try {
      jg = jf.createJsonGenerator(sw);
      m.writeValue(jg, rowCount);
      jsonValue = sw.toString();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Put put = new Put(Bytes.toBytes(Long.MAX_VALUE-System.currentTimeMillis()));//row key change to Long.MAX_VALUE-System.currentTimeMillis(),so the first row will always be the newest
    put.add(TableRowCountTable.FAMILY, TableRowCountTable.QUALIFIER, Bytes.toBytes(jsonValue));
    try {
      HTableInterface rowCountTable = pool.getTableRowCountTable();
      rowCountTable.put(put);
    } catch (IOException e) {
      throw new GdbException("Fail to add tableRowCount." + e);
    }
  }
}
