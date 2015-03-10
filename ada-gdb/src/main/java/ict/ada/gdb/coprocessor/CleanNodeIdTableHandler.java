package ict.ada.gdb.coprocessor;

import ict.ada.common.model.Node;
import ict.ada.common.util.Pair;
import ict.ada.gdb.common.DelEdge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * cleanNodeIdTableHandler ,the  input is idNamePairs
 * first decide the nodes in idNamePairs to delete,
 * then delete the nodes in all of node related tables.
 *
 */
public class CleanNodeIdTableHandler  implements Runnable{
	private static final Log LOG = LogFactory.getLog(CleanNodeIdTableHandler.class);
	public static final List<Pair<byte[],byte[]>> POISON_OBJECT= new ArrayList<Pair<byte[],byte[]>>(0);
	private HRegion region;
	private CountDownLatch latch;
	private BlockingQueue<List<Pair<byte[],byte[]>>> queue;
    private HTable edgeIdTable;
    private HTable nodeNameTable;
    private HTable nodeAttTable;
    private HTable nodeWdeRefTable;
	public CleanNodeIdTableHandler(HRegion region,CountDownLatch latch,BlockingQueue<List<Pair<byte[],byte[]>>> queue,HTable edgeIdTable,HTable nodeNameTable,HTable nodeAttTable,HTable nodeWdeRefTable){
		this.region=region;
		this.latch=latch;
		this.queue=queue;
		this.edgeIdTable=edgeIdTable;
		this.nodeNameTable=nodeNameTable;
		this.nodeAttTable=nodeAttTable;
		this.nodeWdeRefTable=nodeWdeRefTable;
	}
	
	
    private void process(List<Pair<byte[],byte[]>> idNamePairs) throws IOException, Throwable{
       List<Pair<byte[],byte[]>> idNamePairToDel= getIdToDel(idNamePairs);
       List<Delete> idDeletes=new ArrayList<Delete>(idNamePairToDel.size());
       List<Delete> nameDeletes=new ArrayList<Delete>(idNamePairToDel.size());
       for(Pair<byte[],byte[]>idNamePair:idNamePairToDel){
    	   idDeletes.add(new Delete(idNamePair.getFirst()));
    	   if(idNamePair.getSecond()!=null&&idNamePair.getSecond().length!=0)
    		nameDeletes.add(new Delete(idNamePair.getSecond()));   
       }
       //just delete the node in all of the node related tables without  judge weather the node exists in the table;
       // the order matters!
       nodeAttTable.batch(idDeletes);
       nodeWdeRefTable.batch(idDeletes);
       nodeNameTable.batch(nameDeletes);
       for(Delete delete:idDeletes)
    	   region.delete(delete, false);
    }
	
    
    /**this is like the batch method of HTable, decide weather the ids are still exists at once.<br>
     * 
     * @param idNamePairs can not be empty should not be Duplicated!
     * @return
     * @throws IOException
     * @throws Throwable
     */
    private List<Pair<byte[],byte[]>> getIdToDel(List<Pair<byte[],byte[]>>idNamePairs) throws IOException, Throwable{
    	 byte[] idsByte=new byte[idNamePairs.size()*Node.NODEID_SIZE];
         byte[]startRow=idNamePairs.get(0).getFirst();
         byte[]stopRow=idNamePairs.get(idNamePairs.size()-1).getFirst();//inclusive
         int index=0;
         for(Pair<byte[],byte[]> idNamePair:idNamePairs)
         	for(byte b:idNamePair.getFirst())
         		idsByte[index++]=b;
      //   LOG.info("1 ids to judge:"+StringUtils.byteToHexString(idsByte));
         final BytesWritable idsWritable=new BytesWritable(idsByte);
         //the return result is byte array , if the byte==0x00 then the nodeId are not in this region! 
         Map<byte[], BytesWritable> results = edgeIdTable.coprocessorExec(HBaseEdgeDaoProtocol.class, startRow,
         		stopRow,
 		         new Batch.Call<HBaseEdgeDaoProtocol, BytesWritable>() {
 		           public BytesWritable call(HBaseEdgeDaoProtocol instance) { 	    
 						try {
 							return instance.getIdsToDeleteByEdgeIdHTable(idsWritable);
 						} catch (Exception e) {
 							
 						    LOG.error("getIdsToDeleteByEdgeIdHTable", e);
 						}
 						return null;	          
 		           }
 		         });
 	    
     	byte[]result=new byte[idNamePairs.size()];
     	//startRow and endRow may across more than one region,union all the result returned from each region  to get the final result.
     	//if byte == 0x00 in the final result ,then the corresponding node is not exists,
     	for(Map.Entry<byte[], BytesWritable>e:results.entrySet()){
     		byte[] resultRegion =e.getValue().getBytes();
     		for(int i=0;i<idNamePairs.size();i++){
     			result[i]=(byte) (result[i]|resultRegion[i]);
     		}
     	}
      List<Pair<byte[],byte[]>> idNamePairToDel= new ArrayList<Pair<byte[],byte[]>>();
       index=0;
      for(Pair<byte[],byte[]> id:idNamePairs){
    	  if(result[index++]==0x00)
    		  idNamePairToDel.add(id);
      }
      return idNamePairToDel;
    }
	
	
	@Override
	public void run() {
	    while(true){
	    	try {
	    		List<Pair<byte[],byte[]>> idNamePairs = queue.take();
	    		if(idNamePairs==CleanNodeIdTableHandler.POISON_OBJECT){// meet the POISON_OBJECT ,jump out the loop ,end the thread;
	    			queue.put(CleanNodeIdTableHandler.POISON_OBJECT);
	    			latch.countDown();
	    			break;
	    		}
		  // LOG.info("test start to process(idNamePairs)");
				process(idNamePairs);
			
			} catch (InterruptedException e) {
		
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	   
	}
	
	
	
}
