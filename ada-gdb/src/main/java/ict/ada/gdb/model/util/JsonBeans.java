package ict.ada.gdb.model.util;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

class WdeRefBean {
  @JsonProperty("id")
  public String id;

  @JsonProperty("off")
  public String offset;

  @JsonProperty("len")
  public String length;

  @JsonProperty("ts")
  public  String timestamp;
  public WdeRefBean construct(Class<?> clazz,Object entity){
    String id = ReflectionUtil.getField(clazz, this.id, entity);
    if(id != null){
      WdeRefBean bean = new WdeRefBean();
      bean.id= id;
      String off = ReflectionUtil.getField(clazz, this.offset,entity);
      if(off == null) off = "0";
      String length = ReflectionUtil.getField(clazz, this.length,entity);
      if(length == null) {
         length = "0";
        if(!off.equals("0")) 
          off = "0";
      }
      String timestamp = ReflectionUtil.getField(clazz, this.timestamp,entity);
      if(timestamp == null) timestamp = "0";
      bean.offset = off;
      bean.length = length;
      bean.timestamp = timestamp;
      return bean;
    }
    return null;
  }

}

class NodeAttrValueBean {
  @JsonProperty("v")
  public String value;

  @JsonProperty("refs")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<WdeRefBean> wdeRefs;
  
  public NodeAttrValueBean construct(Class<?> clazz,Object entity){
    String value =  ReflectionUtil.getField(clazz, this.value, entity);
    if(value == null) return null;
    NodeAttrValueBean bean = new NodeAttrValueBean();
    bean.value = value;
    if(this.wdeRefs !=null){
      List<WdeRefBean> wdeRefs = new ArrayList<WdeRefBean>();
      for(WdeRefBean wde :this.wdeRefs ){
          WdeRefBean wdeBean = wde.construct(clazz, entity);
          if(wdeBean !=null)
            wdeRefs.add(wdeBean);
      }
      if(wdeRefs.size()!=0)
        bean.wdeRefs = wdeRefs;
    }
  return bean;
  }
}

class NodeAttrBean {
  @JsonProperty("attK")
  public String key;
  
  
  @JsonProperty("attVs")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<NodeAttrValueBean> values;
  
  public NodeAttrBean  construct(Class<?> clazz,Object entity){
     if(this.values == null) return null;
     List<NodeAttrValueBean> values = new ArrayList<NodeAttrValueBean>();
     for(NodeAttrValueBean value : this.values){
       NodeAttrValueBean valueBean = value.construct(clazz, entity);
       if(valueBean!=null)
         values.add(valueBean);
     }
    if(values.size()!=0){
      NodeAttrBean  bean = new NodeAttrBean();
      bean.key = this.key;
      bean.values = values;
      return bean;
    }
    return null;
  }
  
}

/**
 * NodeBean <==> Node
 */
class NodeBean {
  @JsonProperty("name")
  public String name;

  @JsonProperty("sName")
  public List<String> sName;
  
  
  @JsonProperty("addl")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public String additional;
  
  @JsonProperty("type")
  public String type;

  @JsonProperty("attrs")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<NodeAttrBean> attributes;

  @JsonProperty("refs")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<WdeRefBean> wdeRefs;
  public NodeBean  construct(Class<?> clazz,Object entity){
    String name = ReflectionUtil.getField(clazz, this.name,entity);
    String type = ReflectionUtil.getField(clazz, this.type,entity);// TODO how to decide the node type
    if(name == null) return null;
    List<String> sName = new ArrayList<String>(this.sName.size());
    for(String sname : this.sName){
      String snameV = ReflectionUtil.getField(clazz, sname,entity);
      if(snameV!=null) sName.add(snameV);
    }
    String additional = ReflectionUtil.getField(clazz, this.additional,entity);
   
    List<WdeRefBean> wdeRefs =null;
    if(this.wdeRefs!=null){
      wdeRefs= new ArrayList<WdeRefBean>(this.wdeRefs.size());
      for(WdeRefBean wde : wdeRefs){
        WdeRefBean wdeBean = wde.construct(clazz, entity);
        if(wdeBean != null)
            wdeRefs.add(wdeBean);
      } 
    }
   List<NodeAttrBean> attributes = null;
   if(this.attributes!=null){
     attributes = new ArrayList<NodeAttrBean>();
     for(NodeAttrBean attr : this.attributes){
       NodeAttrBean attrBean = attr.construct(clazz, entity);
       if(attrBean !=null)
         attributes.add(attrBean);
     }
   }
   NodeBean bean = new NodeBean();
   bean.name = name;
   bean.sName = sName;
   bean.type =type;
   bean.additional = additional;
   if(wdeRefs !=null && wdeRefs.size()!=0)
     bean.wdeRefs =wdeRefs;
   if(attributes!=null && attributes.size()!=0) 
     bean.attributes = attributes;
   return bean;
  }
}

class RelationBean {
  @JsonProperty("type")
  public String type;

  @JsonProperty("refs")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<WdeRefBean> wdeRefs;
  public RelationBean  construct(Class<?> clazz,Object entity){
    RelationBean bean = new RelationBean();
    bean.type = this.type;
    if(this.wdeRefs !=null){
      List<WdeRefBean> wdeRefs = new ArrayList<WdeRefBean>();
      for(WdeRefBean wde :this.wdeRefs ){
          WdeRefBean wdeBean = wde.construct(clazz, entity);
          if(wdeBean !=null)
            wdeRefs.add(wdeBean);
      }
      if(wdeRefs.size()!=0)
        bean.wdeRefs = wdeRefs;
    }
    return bean;
  }
}

class EdgeBean {
  @JsonProperty("nA")
  public String headName;

  @JsonProperty("tA")
  public String headType;

  @JsonProperty("nB")
  public String tailName;

  @JsonProperty("tB")
  public String tailType;

  @JsonProperty("directed")
  public boolean directed;

  @JsonProperty("rels")
  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<RelationBean> relations;
  public EdgeBean  construct(Class<?> clazz,Object entity){
    
    String headName =  ReflectionUtil.getField(clazz, this.headName,entity);
    String  tailName =  ReflectionUtil.getField(clazz, this.tailName,entity);
    if(headName == null || tailName == null) 
       return null;
      EdgeBean bean = new EdgeBean();
      bean.headName = headName;
      bean.headType = this.headType;
      bean.tailName = tailName;
      bean.tailType = this.tailType;
      bean.directed = this.directed;
      if(this.relations != null){
        List<RelationBean> relations = new ArrayList<RelationBean>();
        for(RelationBean relation: this.relations){
          RelationBean relationBean = relation.construct(clazz, entity);
          if(relationBean != null)
            relations.add(relationBean);
        }
        if(relations!=null && relations.size()!=0)bean.relations = relations;
      }
      return bean;
  }
}
