package ict.ada.gdb.common;

import ict.ada.common.model.Edge;
import ict.ada.gdb.schema.EdgeRelWeightSumHTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;

public class DelEdge {
public static final DelEdge POISON_OBJECT=new DelEdge();
private byte[] id;
private boolean delete=true;
private List<byte[]> edgeIdWithTs; 
private Map<ByteArray,Long> relationTypeWeight;

private DelEdge(){}
public DelEdge(List<KeyValue> kvs){
	byte[] row =kvs.get(0).getRow();
	id=Arrays.copyOfRange(row, 0, Edge.EDGEID_SIZE+1);//with salt
	edgeIdWithTs =new ArrayList<byte[]>();
	edgeIdWithTs.add(kvs.get(0).getRow());
	relationTypeWeight=new HashMap<ByteArray,Long>();
	for(KeyValue kv:kvs){
	      ByteArray type=new ByteArray(kv.getQualifier());
	      relationTypeWeight.put(type, Bytes.toLong(kv.getValue()));
	}
	
	
}
public boolean add(List<KeyValue> kvs){
	byte[] row =kvs.get(0).getRow();
	if(!Bytes.equals(id, 0, id.length, row,0, id.length))return false;
	else {
		edgeIdWithTs.add(row);
	    for(KeyValue kv:kvs){
	    	ByteArray type=new ByteArray(kv.getQualifier());
	        Long weight=relationTypeWeight.get(type);
	    	if(weight!=null){
	    		relationTypeWeight.put(type, weight+Bytes.toLong(kv.getValue()));
	    	}
	    	else relationTypeWeight.put(type, Bytes.toLong(kv.getValue()));
	    	
	    }
	    return true;
	}
//byte[] edgeId=new ByteArray(HBaseEdgeDAO.getEdgeIdFromSaltedRowKey(row));

}
public void reGenDelEdgeFromWeightSum(Result result ){
	 for(KeyValue kv:result.list()){
		 ByteArray type= new ByteArray(kv.getQualifier());
		
		 Long weightD=relationTypeWeight.get(type);
		 if(weightD==null)
			 delete=false;
		 else{
			 Long weight=Bytes.toLong(kv.getValue());
			 if(weight>weightD){relationTypeWeight.put(type, 0-weightD);delete=false;}
			 else relationTypeWeight.put(type,0L ); 
		 }
	 }
}
public byte[] getId() {
	return id;
}
public boolean isDelete() {
	return delete;
}
public Map<ByteArray, Long> getRelationTypeWeight() {
	return relationTypeWeight;
}
public List<byte[]> getEdgeIdWithTs() {
	return edgeIdWithTs;
}


 

}
