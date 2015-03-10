package ict.ada.gdb.coprocessor;

import ict.ada.common.util.Pair;
import ict.ada.gdb.schema.GdbHTableType;
import ict.ada.gdb.schema.NodeIdHTable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

public class HBaseNodeDaoEndPoint extends BaseEndpointCoprocessor implements HBaseNodeDaoProtocol{
	
	private static final Log LOG = LogFactory.getLog(HBaseNodeDaoEndPoint.class);
	private static final int BATCH_SIZE=10000;
	
	
	
	public long cleanNodeHTable()throws IOException{
		
	     // LOG.info("start to delete RelationTypeHTableByTS, the start = "+ start+"s end = "+ end+"s.");
	       HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
	       Scan scan =new Scan();
	       scan.addFamily(NodeIdHTable.FAMILY.getBytes());
	       InternalScanner scanner=null;
	       long  st=System.currentTimeMillis();
	       long count=0;
	       long total=0;
	       LOG.info("start to  cleanNodeHTable in region: +"+region.getRegionNameAsString());
	       BlockingQueue<List<Pair<byte[],byte[]>>> idsQueue=new LinkedBlockingQueue<List<Pair<byte[],byte[]>>>(10);
	       Configuration conf =HBaseConfiguration.create();
	       // edgeIdTable to
	       HTable edgeIdTable= new HTable(conf,region.getTableDesc().getNameAsString().replaceFirst(GdbHTableType.NODE_ID.getContentTag(), GdbHTableType.EDGE_ID.getContentTag()));
	       HTable nodeNameTable =new HTable(conf,region.getTableDesc().getNameAsString().replaceFirst(GdbHTableType.NODE_ID.getContentTag(), GdbHTableType.NODE_NAME.getContentTag()));
	       HTable nodeAttTable =new HTable(conf,region.getTableDesc().getNameAsString().replaceFirst(GdbHTableType.NODE_ID.getContentTag(), GdbHTableType.NODE_ATTR.getContentTag()));
	       HTable nodeWdeRefTable =new HTable(conf,region.getTableDesc().getNameAsString().replaceFirst(GdbHTableType.NODE_ID.getContentTag(), GdbHTableType.NODE_WDEREFS.getContentTag()));
	       edgeIdTable.setAutoFlush(true);
	       nodeNameTable.setAutoFlush(true);
	       nodeAttTable.setAutoFlush(true);
	       nodeWdeRefTable.setAutoFlush(true);
	       CountDownLatch latch=new CountDownLatch(1);
	       CleanNodeIdTableHandler handler = new CleanNodeIdTableHandler(region,latch,idsQueue,edgeIdTable,nodeNameTable,nodeAttTable,nodeWdeRefTable);
	       Thread handlerThread=new Thread(handler);
	       handlerThread.start();
	 	  try {
	 		scanner=region.getScanner(scan);
	 		List<KeyValue> curVals = new ArrayList<KeyValue>();
	 		List<Pair<byte[],byte[]>> idNamePairs =new ArrayList<Pair<byte[],byte[]>>(BATCH_SIZE);
	 		 boolean hasMore = false;
	 		 do {
	              curVals.clear();
	              hasMore = scanner.next(curVals);
	              total++;
	              byte[] id=null;
	              byte[] name=null;
	              for (KeyValue kv : curVals) {
	            	  id=kv.getRow();
	            	  if(Bytes.equals(kv.getQualifier(), NodeIdHTable.QUALIFIER.getBytes()))
	            		  name=kv.getValue();
	              }
	              if(id!=null){
	            	 
	            	  idNamePairs.add(new Pair<byte[],byte[]>(id,name));
	            	  if(idNamePairs.size()==BATCH_SIZE){
	            		  idsQueue.add(idNamePairs);
	            		  idNamePairs=new ArrayList<Pair<byte[],byte[]>>(BATCH_SIZE);
	            	  }
	              }
	            } while (hasMore);
	 		 if(idNamePairs.size()!=0){
	 			//LOG.info("add to queue:"+idNamePairs.size());
	 			 idsQueue.add(idNamePairs);}
	          } catch(Exception e){
	        	  e.printStackTrace();
	          }
	 	  finally {
	 		  try {
	 			 
	 			 idsQueue.put(CleanNodeIdTableHandler.POISON_OBJECT);
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	            scanner.close();
	          }
	 	 LOG.info("finish   cleanNodeHTable in region: +"+region.getRegionNameAsString()+".  in "+(System.currentTimeMillis()-st)+"ms, total rows :"+total+", changed rows :" +count);
			return count;
	  
	}
}
