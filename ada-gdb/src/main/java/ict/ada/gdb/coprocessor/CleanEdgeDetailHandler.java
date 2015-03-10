package ict.ada.gdb.coprocessor;

import ict.ada.common.model.Edge;
import ict.ada.gdb.common.DelEdge;
import ict.ada.gdb.schema.EdgeRelWeightSumHTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;

public class CleanEdgeDetailHandler implements Runnable {
	private static final Log LOG = LogFactory.getLog(CleanEdgeDetailHandler.class);
    private static final int BATCH_SIZE=10000;
	private HTable edgeIdTable;
	private HTable edgeWeightSumTable;
	private HRegion region;
	private CountDownLatch latch;
	private BlockingQueue<DelEdge> queue;
	
	
	public CleanEdgeDetailHandler(HTable edgeIdTable,HTable edgeWeightSumTable,HRegion region,CountDownLatch latch,BlockingQueue<DelEdge> queue){
		this.edgeIdTable=edgeIdTable;
		this.edgeWeightSumTable=edgeWeightSumTable;
		this.region=region;
		this.latch=latch;
		this.queue=queue;
	}
	
	/**steps:<br>
	   * 1, batch get from edgeWeightSumTable for rows that has bean modified in edgeWeightDetailTable.<br>
	   * 2, reGenDelEdge to generate the delete or change(increment) information for edgeIdTable and edgeWeightSumTable.<br>
	   * 3, construct  the batch change list for   edgeIdTable and edgeWeightSumTable.<br>
	   * 4, In a certain order to change the table edgeIdTable -> edgeWeightSumTable -> region(edgeWeightDetailTable)<br>
	   * 
	   * TODO use producer/customer to improve performance!(BlockingQueue)
	   * */
	public void process(List<DelEdge> delEdges){
		  long st=System.currentTimeMillis();
		  List<Delete> edgeIdDeltes=new ArrayList<Delete>();        //
	      List<Delete> edgeWeightDetailDeletes=new ArrayList<Delete>();
	      List<Row> edgeWeightSumChanges=new ArrayList<Row>(); //all in one region,good!
	      List<Get> gets=new ArrayList<Get>(BATCH_SIZE);
	      for(DelEdge delEdge : delEdges){
	    	  for(byte[] row:delEdge.getEdgeIdWithTs()){
	    		  Delete delete =new Delete(row);
	    		  edgeWeightDetailDeletes.add(delete);
	    	  }
	    	  Get get =new Get(delEdge.getId());
	    	  get.addFamily(EdgeRelWeightSumHTable.FAMILY);
	    	  gets.add(get);
	      }
	     try {
			Result[] results= edgeWeightSumTable.get(gets);
			LOG.info("Bach get in edgeWeightSumTable in "+(System.currentTimeMillis()-st)+"ms");
			Iterator<DelEdge> delEdgeIter=delEdges.iterator();
			for(Result result:results){
				DelEdge delEdge=delEdgeIter.next();
				if(result==null||result.isEmpty()){
					delEdgeIter.remove();
				}
				else{
					delEdge.reGenDelEdgeFromWeightSum(result);
				}
			}
			
			for(DelEdge delEdge: delEdges){
				if(delEdge.isDelete()){
					Delete delete=new Delete(Bytes.tail(delEdge.getId(), Edge.EDGEID_SIZE));
					edgeIdDeltes.add(delete);
					Delete delete1=new Delete(delEdge.getId());
					edgeWeightSumChanges.add(delete1);
				}
				else{
				  for(Map.Entry<ByteArray, Long> e:delEdge.getRelationTypeWeight().entrySet()){
					  if(e.getValue()==0){
						  Delete delete1=new Delete(delEdge.getId());
						  delete1.deleteColumns(EdgeRelWeightSumHTable.FAMILY, e.getKey().getBytes());
					  }
					  else if(e.getValue()<0){
						  Increment increment=new Increment(delEdge.getId());
						  increment.addColumn(EdgeRelWeightSumHTable.FAMILY, e.getKey().getBytes(),e.getValue());
						  edgeWeightSumChanges.add(increment);
					  }
				  }	
				}
			}
			try {
				st=System.currentTimeMillis();
				Result[] results1=new Result[edgeIdDeltes.size()]; //all result should be result.empty
				Result[] results2=new Result[edgeWeightSumChanges.size()];
				edgeIdTable.batch(edgeIdDeltes,results1);// Just use the retries of HBase
				LOG.info("Bach delete in edgeIdTable in "+(System.currentTimeMillis()-st)+"ms");
				st=System.currentTimeMillis();
				edgeWeightSumTable.batch(edgeWeightSumChanges,results2);
				LOG.info("Bach changes in edgeWeightSumTable in "+(System.currentTimeMillis()-st)+"ms");
				for(Delete delete :edgeWeightDetailDeletes){
					region.delete(delete, false);
				}
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (IOException e) {
			// TODO what?just ignore;
			e.printStackTrace();
		}
	  
	}
	
	@Override
	public void run() {
		List<DelEdge> delEdges =new ArrayList<DelEdge>(BATCH_SIZE);
	    while(true){
	    	try {
	    		DelEdge delEdge = queue.take();
	    		if(delEdge==DelEdge.POISON_OBJECT){// meet the POISON_OBJECT ,jump out the loop ,end the thread;
	    			queue.put(DelEdge.POISON_OBJECT);
	    			if(delEdges.size()!=0)
	    				process(delEdges);
	    			latch.countDown();
	    			break;
	    		}
				delEdges.add(delEdge);
				if(delEdges.size()>=BATCH_SIZE){
					process(delEdges);
					delEdges.clear();}
			} catch (InterruptedException e) {
		
			}
	    }
	   
	}
}
