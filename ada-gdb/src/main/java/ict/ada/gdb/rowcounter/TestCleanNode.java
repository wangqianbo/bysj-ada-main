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
import ict.ada.gdb.coprocessor.HBaseNodeDaoProtocol;
import ict.ada.gdb.schema.NodeIdHTable;

public class TestCleanNode {
	private static Log LOG = LogFactory.getLog(TestCleanNode.class);
private static	Configuration configuration =null;
	static{
	   configuration = HBaseConfiguration.create();
	   configuration.setLong("hbase.rpc.timeout", 6000000);
	}
	
	
	private long cleanNodeHTable(Channel channel){
		HTable table = null;
		try {
			table = new HTable(configuration,NodeIdHTable.getName(NodeType.getType(channel, Attribute.PERSON)));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 Map<byte[], Long> results=null;
		 long count=0;
		 try{
		 long st = Timer.now();
			results = table.coprocessorExec(HBaseNodeDaoProtocol.class, null,
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
	       LOG.info(Bytes.toString(table.getTableName()) + ":clean complete in  "
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
		TestCleanNode test=new TestCleanNode();
		test.cleanNodeHTable(Channel.DATA);
	}

}
