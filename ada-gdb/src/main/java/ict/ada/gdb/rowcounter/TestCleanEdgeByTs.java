package ict.ada.gdb.rowcounter;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.common.util.Timer;
import ict.ada.gdb.common.TimeRange;
import ict.ada.gdb.coprocessor.HBaseEdgeDaoProtocol;
import ict.ada.gdb.schema.EdgeRelWeightDetailHTable;
import ict.ada.gdb.schema.RelationTypeHTable;
import ict.ada.gdb.schema.RelationWdeRefsHTable;

public class TestCleanEdgeByTs {
	private static Log LOG = LogFactory.getLog(TestCleanEdgeByTs.class);
private static	Configuration configuration =null;
	static{

   
	   configuration = HBaseConfiguration.create();
	   configuration.setLong("hbase.rpc.timeout", 6000000);
	}
	
	
	private long deleteRelationTypeHTableByTS(Channel channel,TimeRange timeRange){
		if(timeRange==TimeRange.ANY_TIME) return 0L;//will not support ANY_TIME
		final byte[] start = Bytes.toBytes((int)timeRange.getStartInclusiveInSec());
		final byte[] end = Bytes.toBytes((int)timeRange.getEndExclusiveInSec());
		HTable table = null;
		try {
			table = new HTable(configuration,RelationWdeRefsHTable.getName(NodeType.getType(channel, Attribute.PERSON)));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 Map<byte[], Long> results=null;
		 long count=0;
		 try{
		 long st = Timer.now();
			results = table.coprocessorExec(HBaseEdgeDaoProtocol.class, null,
			         null,
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
		   
		   for(Map.Entry<byte[], Long>e:results.entrySet()){
			   count+=e.getValue();
		   }
	       LOG.info(Bytes.toString(table.getTableName()) + ":delete complete in  "
	           + Timer.msSince(st) + "ms"+", the number of row delete :"+count+". " );
		 }catch(Throwable e){
			 e.printStackTrace();
		 }
		 return count;
	}
	
	
	
	private long deleEdgeIdAndEdgeSumTable(Channel channel,TimeRange timeRange){
 
		if(timeRange==TimeRange.ANY_TIME) return 0L;//will not support ANY_TIME
		final byte[] start = Bytes.toBytes((int)timeRange.getStartInclusiveInSec());
		final byte[] end = Bytes.toBytes((int)timeRange.getEndExclusiveInSec());
		//HTableInterface relationTypeHTable = pool.getEdgeRelWeightDetailTable(NodeType.getType(channel, Attribute.PERSON));
		HTable table = null;
		try {
			table = new HTable(configuration,EdgeRelWeightDetailHTable.getName(NodeType.getType(channel, Attribute.PERSON)));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 Map<byte[], Long> results=null;
		 long count=0;
		 try{
		 long st = Timer.now();
			results = table.coprocessorExec(HBaseEdgeDaoProtocol.class, null,
			         null,
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
		   
		   for(Map.Entry<byte[], Long>e:results.entrySet()){
			   count+=e.getValue();
		   }
	       LOG.info(Bytes.toString(table.getTableName()) + ":delete complete in  "
	           + Timer.msSince(st) + "ms"+", the number of row delete :"+count+". " );
		 }catch(Throwable e){
			 e.printStackTrace();
		 }
		 return count;
	
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int start=Integer.parseInt(args[0]);
		int end=Integer.parseInt(args[1]);
		//AdaGdbService service = new AdaGdbService();
		TimeRange timeRange=new TimeRange(start,end);
		TestCleanEdgeByTs test=new TestCleanEdgeByTs();
		test.deleteRelationTypeHTableByTS(Channel.NEWS, timeRange);
		test.deleEdgeIdAndEdgeSumTable(Channel.NEWS, timeRange);
	}

}
