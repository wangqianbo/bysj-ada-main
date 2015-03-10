package ict.ada.gdb.schema.util;

import java.util.Map;

import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.service.AdaGdbService;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

public class Test {
public static void main(String[] args) throws GdbException{
	AdaGdbService service=new AdaGdbService();
	Map<String,byte[]> result=service.getRelationType(Channel.HUDONGBAIKE);
	for(Map.Entry<String, byte[]>e:result.entrySet()){
		System.out.println(e.getKey()+" :  "+StringUtils.byteToHexString(e.getValue()));
	}
	}
}
