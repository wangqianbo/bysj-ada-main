package ict.ada.gdb.dataloader;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.NodeAttribute;
import ict.ada.common.model.NodeAttribute.AttrValueInfo;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.RelationType;
import ict.ada.common.model.WdeRef;
import ict.ada.common.util.Hex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

class WdeRefBean {
  @JsonProperty("id")
  public String id;

  @JsonProperty("off")
  public int offset;

  @JsonProperty("len")
  public int length;

  @JsonProperty("ts")
  public int timestamp;

  public static WdeRef convert(WdeRefBean wrfBean) throws GdbDataLoaderException {
    String checkResult = check(wrfBean);
    if (null != checkResult)
      throw new GdbDataLoaderException("check WdeRefBean failed. Cause: " + checkResult);
    // System.out.println(wrfBean.id);
    // System.out.println(wrfBean.length);
    WdeRef ref = new WdeRef(Hex.decodeHex(wrfBean.id), wrfBean.offset, wrfBean.length,
        wrfBean.timestamp);
    // System.out.println("[WdeRef]" + ref);
    return ref;
  }

  private static String check(WdeRefBean wrfBean) {
    if (wrfBean == null) return "null WdeRefBean";
    if (wrfBean.id == null) return "null id";
    if (wrfBean.id.length() != WdeRef.WDEID_SIZE * 2)
      return "id length is " + wrfBean.id.length() + ". It should be " + WdeRef.WDEID_SIZE * 2;
    if (wrfBean.offset < 0 || wrfBean.length < 0)
      return "off" + wrfBean.offset + " len=" + wrfBean.length;
    if (wrfBean.offset > 0 && wrfBean.length == 0)
      return "off" + wrfBean.offset + " len=" + wrfBean.length;
    // offset=0,length=0 means no offset and length info, which is legal.
    if (wrfBean.timestamp < 0) return "timestamp=" + wrfBean.timestamp;
    return null;
  }
}

/**
 * NodeAttrValueBean <==> AttrValueInfo
  @JsonProperty("id")
  public String id;

  @JsonProperty("off")
  public int offset;

  @JsonProperty("len")
  public int length;

  @JsonProperty("ts")
  public int timestamp;

  public static WdeRef convert(WdeRefBean wrfBean) throws GdbDataLoaderException {
    String checkResult = check(wrfBean);
    if (null != checkResult)
      throw new GdbDataLoaderException("check WdeRefBean failed. Cause: " + checkResult);
    // System.out.println(wrfBean.id);
    // System.out.println(wrfBean.length);
    WdeRef ref = new WdeRef(Hex.decodeHex(wrfBean.id), wrfBean.offset, wrfBean.length,
        wrfBean.timestamp);
    // System.out.println("[WdeRef]" + ref);
    return ref;
  }

  private static String check(WdeRefBean wrfBean) {
    if (wrfBean == null) return "null WdeRefBean";
    if (wrfBean.id == null) return "null id";
    if (wrfBean.id.length() != WdeRef.WDEID_SIZE * 2)
      return "id length is " + wrfBean.id.length() + ". It should be " + WdeRef.WDEID_SIZE * 2;
    if (wrfBean.offset < 0 || wrfBean.length < 0)
      return "off" + wrfBean.offset + " len=" + wrfBean.length;
    if (wrfBean.offset > 0 && wrfBean.length == 0)
      return "off" + wrfBean.offset + " len=" + wrfBean.length;
    // offset=0,length=0 means no offset and length info, which is legal.
    if (wrfBean.timestamp < 0) return "timestamp=" + wrfBean.timestamp;
    return null;
  }

 */
class NodeAttrValueBean {
  @JsonProperty("v")
  public String value;

  @JsonProperty("refs")
  public List<WdeRefBean> wdeRefs;

  public static AttrValueInfo convert(NodeAttrValueBean navb) throws GdbDataLoaderException {
    String checkResult = check(navb);
    if (null != checkResult)
      throw new GdbDataLoaderException("NodeAttrValueBean check failed. Cause: " + checkResult);
    List<WdeRef> refList = null;
    if (navb.wdeRefs != null) {
      refList = new ArrayList<WdeRef>(navb.wdeRefs.size());
      for (WdeRefBean wrfBean : navb.wdeRefs) {
        refList.add(WdeRefBean.convert(wrfBean));
      }
    }
    return new AttrValueInfo(navb.value, refList);

  }

  private static String check(NodeAttrValueBean navb) {
    if (navb == null) return "null NodeAttrValueBean";
    if (navb.value == null || navb.value.equals(""))
      return "null or empty NodeAttrValueBean value";
    // TODO can navb.wdeRefs be null?
    // if (navb.wdeRefs == null || navb.wdeRefs.size() == 0) return false;
    return null;
  }
}

class NodeAttrBean {
  @JsonProperty("attK")
  public String key;

  @JsonProperty("attVs")
  public List<NodeAttrValueBean> values;

  public static NodeAttribute convert(NodeAttrBean nab) throws GdbDataLoaderException {
    String checkResult = check(nab);
    if (null != checkResult)
      throw new GdbDataLoaderException("NodeAttrBean check failed. Cause: " + checkResult);
    List<AttrValueInfo> attrValues = new ArrayList<AttrValueInfo>(nab.values.size());
    for (NodeAttrValueBean navb : nab.values) {
      attrValues.add(NodeAttrValueBean.convert(navb));
    }
    return new NodeAttribute(nab.key, attrValues);
  }

  private static String check(NodeAttrBean nab) {
    if (nab == null) return "null NodeAttrBean";
    if (nab.key == null || nab.key.length() == 0) return "null or empty key";
    if (nab.values == null || nab.values.size() == 0) return "null or empty values";
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
  public List<String> additional;
  
  @JsonProperty("type")
  public int type;

  @JsonProperty("attrs")
  public List<NodeAttrBean> attributes;
  
  @JsonProperty("ents")
  public List<String> ents;
  
  @JsonProperty("refs")
  public List<WdeRefBean> wdeRefs;

  public static Node convert(NodeBean nb) throws GdbDataLoaderException {
    String checkResult = check(nb);
    if (null != checkResult)
      throw new GdbDataLoaderException("NodeBean check failed. Cause: " + checkResult);
    Node node = new Node(NodeType.getType(nb.type), nb.name);
    if (nb.sName != null) {
      for (String sn : nb.sName) {
        node.addSearchName(sn);
      }
    }
    if(nb.additional!=null){
        for (String ad : nb.additional) {
          node.addAdditional(ad);
        }
    }
    if(nb.ents != null){
    	for(String ent :  nb.ents){
    		if(ent.length() == Node.NODEID_SIZE*2){
    			node.addEnts(ent);
    		}
    		
    	}
    }
    if (nb.attributes != null) {
      // System.out.println("attrs:" + nb.attributes.size());
      for (NodeAttrBean nab : nb.attributes) {
        if (nab != null) {
          node.addNodeAttribute(NodeAttrBean.convert(nab));
        }
      }
    }
    if (nb.wdeRefs != null) {
      for (WdeRefBean wrfBean : nb.wdeRefs) {
        node.addWdeRef(WdeRefBean.convert(wrfBean));
      }
    }
    return node;
  }

  /**
   * Check the given NodeBean
   */
  private static String check(NodeBean nb) {
    if (nb == null) return "null NodeBean";
    if (nb.name == null || nb.name.length() == 0) return "null or empty node name";
    NodeType nType = NodeType.getType(nb.type);
    if (nType == null || nType.isAggregateType()) return "unknown NodeType: " + nb.type;
    // if (nb.attributes == null && !nType.isAttrType()) return false;
    // TODO checks about wdeRefs. Some relType can have empty wdeRefs.
    return null;
  }
}

class RelationBean {
  @JsonProperty("type")
  public String type;

  @JsonProperty("refs")
  public List<WdeRefBean> wdeRefs;

  /*
   * Relations must be add by an Edge, so there's no convert() method here.
   */

  protected static String check(RelationBean relBean) {
    if (relBean == null) return "null RelationBean";
    RelationType rType = RelationType.getType(relBean.type);//不存在为null的这种情况
    if (rType == null) return "unknown RelationType: " + relBean.type;
    // TODO checks about wdeRefs
    return null;
  }
}

class EdgeBean {
  @JsonProperty("nA")
  public String headName;

  @JsonProperty("tA")
  public int headType;

  @JsonProperty("nB")
  public String tailName;

  @JsonProperty("tB")
  public int tailType;

  @JsonProperty("directed")
  public boolean directed;

  @JsonProperty("rels")
  public List<RelationBean> relations;

  public static List<Edge> convert(EdgeBean eb) throws GdbDataLoaderException {
    String checkResult = check(eb);
    if (null != checkResult)
      throw new GdbDataLoaderException("EdgeBean check failed. Cause: " + checkResult);
    Node nA = new Node(NodeType.getType(eb.headType), eb.headName);
    Node nB = new Node(NodeType.getType(eb.tailType), eb.tailName);
    List<Edge> edges;
    if (eb.directed) {
      edges = Collections.singletonList(new Edge(nA, nB));
    } else {
      edges = new ArrayList<Edge>(2);
      edges.add(new Edge(nA, nB));
      edges.add(new Edge(nB, nA));// add two Edges in both directions
    }
    for (RelationBean rb : eb.relations) {
      for (Edge e : edges) {
        if (rb.wdeRefs != null) {// Some relations may NOT contain wdeRefs
          List<WdeRef> refList = new ArrayList<WdeRef>(rb.wdeRefs.size());
          for (WdeRefBean wrf : rb.wdeRefs) {
            refList.add(WdeRefBean.convert(wrf));
          }
          e.addRelation(RelationType.getType(rb.type), rb.wdeRefs.size(), refList);
        } else {
          e.addRelation(RelationType.getType(rb.type), 1);// weight set to 1
        }
      }
    }
    return edges;
  }

  private static String check(EdgeBean eb) {
    if (eb == null) return "null EdgeBean";
    if (eb.headName == null || eb.headName.length() == 0) return "null or empty headName";
    NodeType nTypeA = NodeType.getType(eb.headType);
    if (nTypeA == null || nTypeA.isAggregateType()) return "unknown TypeA";
    if (eb.tailName == null || eb.tailName.length() == 0) return "null or empty tailName";
    NodeType nTypeB = NodeType.getType(eb.tailType);
    if (nTypeB == null || nTypeB.isAggregateType()) return "unknown TypeB";
    if (eb.relations == null || eb.relations.size() == 0) return "no Relations";
    for (RelationBean relB : eb.relations) {
      String result = RelationBean.check(relB);
      if (null != result) return result;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    // String x = "{\"id\":1,\"off\":0,\"len\":0,\"ts\":0}";
    // WdeRefBean wrb = mapper.readValue(x.getBytes(), WdeRefBean.class);
    // System.out.println(wrb.id);
    // NodeAttrBean nab = new NodeAttrBean();
    // nab.wdeRefs[0] = wrb;
    // nab.wdeRefs[1] = wrb;
    // nab.wdeRefs[2] = wrb;
    // mapper.writeValue(System.out, nab);

    // String bb =
    // "{\"v\":null,\"refs\":[{\"id\":1,\"off\":0,\"len\":0,\"ts\":0},{\"id\":1,\"off\":0,\"len\":0,\"ts\":0},{\"id\":1,\"off\":0,\"len\":0,\"ts\":0}]}";
    String bb = "{\"v\":null,\"refs\":[{\"id\":1,\"off\":0,\"len\":0,\"ts\":0}}";
    NodeAttrValueBean xxx = mapper.readValue(bb.getBytes(), NodeAttrValueBean.class);
    mapper.readValue("", NodeAttrValueBean.class);
    // System.out.println(Arrays.toString(xxx.wdeRefs));
    System.out.println(xxx.wdeRefs.size());

  }

}
