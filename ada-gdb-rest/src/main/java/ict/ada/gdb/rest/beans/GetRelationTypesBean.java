package ict.ada.gdb.rest.beans;

import ict.ada.gdb.util.NodeIdConveter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class GetRelationTypesBean {
  private String errorCode = "success";
  
  private List<RelationType> relationTypes;
  
  public GetRelationTypesBean(Map<String,byte[]> results){
    relationTypes = new ArrayList<RelationType>(results.size());
    for(Entry<String,byte[]> e:results.entrySet()){
      relationTypes.add(new RelationType(e.getKey(),e.getValue()) );
    }
    }

  
  public String getErrorCode() {
    return errorCode;
  }


  public List<RelationType> getRelationTypes() {
    return relationTypes;
  }


  public static class RelationType{
    private String type;
    private String edgeId;
    public RelationType(String type,byte[] edgeId){
      this.type=type;
      this.edgeId = NodeIdConveter.toString(edgeId);
    }
    public String getType() {
      return type;
    }
    public String getEdgeId() {
      return edgeId;
    }
    
  }
}
