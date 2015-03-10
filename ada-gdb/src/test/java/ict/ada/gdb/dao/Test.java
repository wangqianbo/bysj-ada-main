package ict.ada.gdb.dao;

import java.util.HashMap;
import java.util.Map;

public class Test {
 public static void main(String[] args){
   Map<String,String> test = new HashMap<String,String>();
   String v = (String)test.get("1");
   if(v == null) 
     System.out.println("jiong");
 }
}
