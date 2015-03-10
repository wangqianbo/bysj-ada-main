package ict.ada.gdb.common;

import ict.ada.gdb.schema.NodeAttributeHTable;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class BytesTest {

  public static void main(String[] args) {
	  String action ="action";
	  byte[] a=Bytes.add(action.getBytes(),NodeAttributeHTable.DELIMITER );
	  System.out.println(a.length);
	  System.out.println(action.getBytes().length==a.length&&Bytes.equals(action.getBytes(), 0, a.length, a, 0, a.length));
  }

}
