package ict.ada.gdb.common;

import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.RelationType;
import ict.ada.common.model.Node;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hdfs.util.ByteArray;

/**
 * Specification for a Relation Graph Query
 */
public class RelQuerySpec {

  private final Node centerNode;// center node for the RelationGraph

  private final Set<RelationType> requiredRelType;// required Relation type on Edge
  private final Set<ByteArray> containedWdeIds;
  private final Attribute requiredAttribute;// required Node type on the other end of Edge
  private final TimeRange timeRange;
  private final int edgeWeightMin;// min weight for an edge, inclusive
  private final int edgeWeightMax;// max weight for an edge, inclusive
  private final int resultSize;// Edge count in the result RelationGraph
  private final boolean useRelRank;// whether to get Top-n Edges according to weights.
  
  private final boolean critical;// critical queries NEVER use data in GdbCache. Mimic Facebook TAO. 
  
  // Builder Pattern
  public static class RelQuerySpecBuilder {
    // Required fields.
    private final Node centerNode;
    private  Set<ByteArray> containedWdeIds=null;
    // Optional fields with DEFAULT values.

    private Set<RelationType> requiredRelType = null;// null means all types are acceptable.

    private Attribute requiredAttribute = Attribute.ANY;// TODO
    private int edgeWeightMin = Integer.MIN_VALUE;
    private int edgeWeightMax = Integer.MAX_VALUE;
    private int resultSize = 10000;// bigger?
    private TimeRange timeRange = TimeRange.ANY_TIME;
    private boolean useRelRank = false;// disable rank by default.
    private boolean critical = false; //TODO avoid GdbCache by default
    
    public RelQuerySpecBuilder(Node centerNode) {
      this.centerNode = centerNode;
    }

    public RelQuerySpecBuilder relType(RelationType relType) { // this method is modified without changing the name,the function of it like addRelType
      if (requiredRelType == null) requiredRelType = new HashSet<RelationType>();
      this.requiredRelType.add(relType);
      return this;
    }
    public RelQuerySpecBuilder wdeId(byte[] wdeId){
    	if(containedWdeIds==null) containedWdeIds =new HashSet<ByteArray>();
    	this.containedWdeIds.add(new ByteArray(wdeId));
    	return this;
    }
    public RelQuerySpecBuilder attribute(Attribute attribute) {
      this.requiredAttribute = attribute;
      return this;
    }

    public RelQuerySpecBuilder resultSize(int resultSize) {
      this.resultSize = resultSize;
      return this;
    }

    public RelQuerySpecBuilder timeRange(TimeRange range) {
      this.timeRange = range;
      return this;
    }

    public RelQuerySpecBuilder useRelRank(boolean useRelRank) {
      this.useRelRank = useRelRank;
      return this;
    }

    /**
     * Whether this query is critical. Critical queries NEVER use GdbCache
     */
    public RelQuerySpecBuilder isCritical(boolean isCritical) {
      this.critical = isCritical;
      return this;
    }

    /**
     * Min weight for an Edge, inclusive
     */
    public RelQuerySpecBuilder minWeight(int min) {
      this.edgeWeightMin = min;
      return this;
    }

    /**
     * Max weight for an Edge, inclusive
     */
    public RelQuerySpecBuilder maxWeight(int max) {
      this.edgeWeightMin = max;
      return this;
    }
    
    public RelQuerySpec build() {
      return new RelQuerySpec(this);
    }
    

  }

  private RelQuerySpec(RelQuerySpecBuilder builder) {
    this.centerNode = builder.centerNode;
    this.resultSize = builder.resultSize;
    this.timeRange = builder.timeRange;
    this.edgeWeightMin = builder.edgeWeightMin;
    this.edgeWeightMax = builder.edgeWeightMax;
    this.critical = builder.critical;
    this.useRelRank = builder.useRelRank;
    this.requiredRelType = builder.requiredRelType;
    this.requiredAttribute = builder.requiredAttribute;
    this.containedWdeIds=builder.containedWdeIds;
    // Check fields
    if (centerNode == null) throw new NullPointerException("null centerNode");
    if (false == centerNode.validate()) throw new IllegalArgumentException("illegal node="
        + centerNode.toString());
    if (resultSize <= 0) throw new IllegalArgumentException("illegal resultSize=" + resultSize);
    if (timeRange == null) throw new NullPointerException("null TimeRange");
    // if (requiredRelType == null) throw new NullPointerException("null EdgeType");
    if (requiredAttribute == null) throw new NullPointerException("null Attribute");
    if (edgeWeightMin > edgeWeightMax)
      throw new IllegalArgumentException("Edge weight min=" + edgeWeightMin + " max="
          + edgeWeightMax);
  }

  public Node getCenterNode() {
    return centerNode;
  }

  public Set<RelationType> getRequiredRelType() {
    return requiredRelType;
  }

  

  public Set<ByteArray> getContainedWdeIds() {
	return containedWdeIds;
}

public Attribute getRequiredAttribute() {
	return requiredAttribute;
}

public TimeRange getTimeRange() {
    return timeRange;
  }

  public int getResultSize() {
    return resultSize;
  }

  public int getEdgeWeightMin() {
    return edgeWeightMin;
  }

  public int getEdgeWeightMax() {
    return edgeWeightMax;
  }

  public boolean isCritical() {
    return critical;
  }

  /**
   * If true, return resultSize Edges with top relation weights.<br>
   * If false, return resultSize Edges randomly.
   * <p>
   * False by default.
   */
  public boolean isRelRankEnabled() {
    return useRelRank;
  }
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[RelQuerySpec] ");
    try {
      for (Field f : this.getClass().getDeclaredFields()) {
        if (!Modifier.isStatic(f.getModifiers())) {
          sb.append(f.getName() + "=" + f.get(this) + " ");
        }
      }
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }
    return sb.toString();
  }

}
