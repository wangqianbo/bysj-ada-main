package ict.ada.gdb.rest.util;

import ict.ada.common.model.WdeRef;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

public class WDEUtil {
public static  int getTimeStamp(WdeRef wdeRef){
  return Bytes.toInt(wdeRef.getWdeId(),2,4);
}
public static int getLatestTimeStamp(List<WdeRef> wdeRefs){
  if(wdeRefs==null||wdeRefs.size()==0)
    return -1;
  int maxTS = -1;
  for(WdeRef wdeRef : wdeRefs){
    int ts = getTimeStamp(wdeRef);
    if(ts > maxTS) maxTS = ts;
  }
  return maxTS;
}
}
