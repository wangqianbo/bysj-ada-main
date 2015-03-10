package ict.ada.gdb.dataloader;

import java.io.IOException;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import ict.ada.common.model.Node;
import ict.ada.gdb.model.util.PojoMapper;

public class NodeMapper {
  public static Node toNode(String json) throws JsonMappingException, JsonParseException, IOException, GdbDataLoaderException{
    NodeBean bean = (NodeBean) PojoMapper.fromJson(json, NodeBean.class);
    Node node = NodeBean.convert(bean);
    return node;
  }
}
