package ict.ada.gdb.coprocessor;

import ict.ada.gdb.common.RelQuerySpecWritable;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;

//A sample protocol for performing aggregation at regions.
public  interface HBaseEdgeDaoProtocol extends CoprocessorProtocol {
//Perform aggregation for a given column at the region. The aggregation
//will include all the rows inside the region. It can be extended to
//allow passing start and end rows for a fine-grained aggregation.
public long sum(byte[] family, byte[] qualifier) throws IOException;
public Result queryRelGraphRelationWdeRefsHTable(RelQuerySpecWritable relQuerySpec )throws IOException;
public Result queryResultInEdgeRelWeightDetailHTable(RelQuerySpecWritable specWritable)throws Exception;
public MapWritable queryResultInEdgeRelWeightDetailHTableDetail(RelQuerySpecWritable specWritable)  throws Exception;
public MapWritable queryRelGraphRelationWdeRefsHTableDetail(RelQuerySpecWritable specWritable) throws IOException;
public MapWritable getRelationType() throws IOException;
/**
 * @param start ts tart in seconds 
 * @param end   ts end in seconds
 * @return   the number of deletes
 * @throws IOException
 */
public long cleanRelationTypeHTableByTS(byte[] start,byte[] end)throws IOException;
public long cleanEdgeRelWeightDetailHTableByTs(byte[] start,byte[] end)throws IOException;
public long cleanEdgeRelWeightDetailHTableByTs1(byte[] start,byte[] end)throws IOException;
public BytesWritable getIdsToDeleteByEdgeIdHTable(BytesWritable ids)throws IOException;
}