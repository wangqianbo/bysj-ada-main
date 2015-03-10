package ict.ada.gdb.rest.services;

import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.rest.beans.GetRelationTypesBean;
import ict.ada.gdb.rest.beans.RowCountBean;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.rest.util.PojoMapper;
import ict.ada.gdb.rowcounter.TableRowCount;
import ict.ada.gdb.service.AdaGdbService;

import java.util.Map;

public class InternalStateService {
  private static AdaGdbService adaGdbService = InternalServiceResources.getAdaGdbService();

  public static String getRelationTypes(String channelName){
    String ret = null;
    try{
      Channel channel = NodeTypeMapper.getChannel(channelName);
      Map<String,byte[]>result = adaGdbService.getRelationTypeV1(channel);
      GetRelationTypesBean bean = new GetRelationTypesBean(result);
      ret = PojoMapper.toJson(bean, true);
    }catch (GdbException e) {
      return InternalNodeService.generateErrorCodeJson("GdbException happens in query: "
          + e.getMessage());
    }
    return ret;
  }
  
  public static String getTableRowCount(String channelName) {
    String ret = null;
    TableRowCount tableRowCount = null;
    try {
      tableRowCount = adaGdbService.getTableRowCount();
    } catch (GdbException e) {
      return InternalNodeService.generateErrorCodeJson("GdbException happens in query: "
          + e.getMessage());
    }
    RowCountBean bean = null;

    if (channelName.equals("all")) {
      bean = new RowCountBean(tableRowCount);
    } else {
      Channel channel = null;
      try {
        channel = NodeTypeMapper.getChannel(channelName);
      } catch (Exception e) {
        return InternalServiceResources.generateErrorCodeJson(e.getMessage());
      }
      if (channel != Channel.ANY) {
        bean = new RowCountBean();
        bean.setTotalEdges(tableRowCount.getTotalEdgeCount());
        bean.setTotalNodes(tableRowCount.getTotalNodeCount());
        if (tableRowCount.getChannelRowCount().get(channel.getIntForm()) != null) bean.addChannel(
            NodeTypeMapper.getChannelName(channel),
            tableRowCount.getChannelRowCount().get(channel.getIntForm()));
        // TODO 如果该通道没有数据呢?
      }

    }

    ret = PojoMapper.toJson(bean, true);
    return ret;
  }
}
