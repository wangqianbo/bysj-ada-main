package ict.ada.gdb.coprocessor;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.regionserver.HRegion;

public class CleanRelationWdeRefTableHandler implements Runnable{
	private static final Log LOG = LogFactory.getLog(CleanRelationWdeRefTableHandler.class);
	public static final Delete POISON_OBJECT= new Delete();
	private HRegion region;
	private CountDownLatch latch;
	private BlockingQueue<Delete> queue;
	public CleanRelationWdeRefTableHandler(HRegion region,CountDownLatch latch,BlockingQueue<Delete> queue){
		this.region=region;
		this.latch=latch;
		this.queue=queue;
	}
	@Override
	public void run() {
		while (true) {
			try {
				Delete delete = queue.take();
				if (delete == POISON_OBJECT) {// meet the POISON_OBJECT ,jump out
											// the loop ,end the thread;
					queue.put(POISON_OBJECT);
					latch.countDown();
					break;
				} 
				else region.delete(delete, false);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	public static void main(String[] args){
		Row test=new Delete(new byte[]{0x01});
		if(test instanceof Delete)
			System.out.println(true);
	}
	}
