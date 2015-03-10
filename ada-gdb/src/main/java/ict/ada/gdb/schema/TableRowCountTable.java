package ict.ada.gdb.schema;

import ict.ada.common.model.NodeType;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Set;

public class TableRowCountTable {
//Schema
 public static final byte[] FAMILY = "i".getBytes();
 public static final byte[] QUALIFIER = "i".getBytes();

 public static String getName() {
   return "gdb-tablerowcount";
 }
 public static void main(String[] args) throws UnsupportedEncodingException{
   String s="253%2Fdfdsfdsf%";
   s=s.replaceAll("%", "%25");
   System.out.println(s);
   String nameDecoded = URLDecoder.decode(s, "UTF-8");
   System.out.println(nameDecoded);
 }
}
