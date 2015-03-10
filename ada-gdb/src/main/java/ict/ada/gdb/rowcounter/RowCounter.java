package ict.ada.gdb.rowcounter;

import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.dao.HBaseAggregationDao;
import ict.ada.gdb.dao.HBaseDAOFactory;

public class RowCounter {
public static void main(String[] args){
  HBaseAggregationDao aggregationDao= HBaseDAOFactory.getHBaseAggregationDao();
  try {
	  AdaModeConfig.setMode(AdaModeConfig.GDBMode.QUERY);
    aggregationDao.addTableRowCount();
  } catch (GdbException e) {
    // TODO Auto-generated catch block
  System.out.println(e);
  }
}
}
