package ict.ada.gdb.dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


public class GetName {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//AdaGdbService service =new AdaGdbService();
		Configuration conf = HBaseConfiguration.create() ;
		String tableName=args[0];
		try {
			HTable nameTable=new HTable(conf,tableName);
			Scan scan =new Scan();
			//byte[] row=StringUtils.hexStringToByte(args[0]);
			ResultScanner scanner=nameTable.getScanner(scan);
			int rowCount=0;
			int nullCount=0;
			for(Result result:scanner){
				rowCount++;
				if(result.getValue("i".getBytes(),"v".getBytes())==null){
					nullCount++;
				}
			}
			System.out.println("rowCount="+rowCount+"  nullCount="+nullCount);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
