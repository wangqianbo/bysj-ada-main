package ict.ada.gdb.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface HBaseNodeDaoProtocol  extends CoprocessorProtocol{
	
	/**
	 * use nodeId to scan EdgeIdTable, if the nodeId does not have any edge that related to it ,
	 * then delete the node[in nodeName nodeId nodeAttr nodeWdeRef table].
	 * 
	 * */
	public long cleanNodeHTable()throws IOException;
	

}
