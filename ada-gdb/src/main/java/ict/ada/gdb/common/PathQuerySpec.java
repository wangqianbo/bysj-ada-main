package ict.ada.gdb.common;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.RelationType;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Specification for a Path Query.<br>
 */
public class PathQuerySpec {

  private final Node startNode;// Shared start node of all paths
  private final Node endNode;// Shared end node of all paths
  private final Attribute requiredAttribute;// type of nodes in paths
  private final RelationType requiredRelType;// required Relation type on Edge
  private final TimeRange timeRange;// time range for calculate weights
  private final int maxPathLength;// max length of a path
  private final long maxEdgeWeight;// max weight in Path edges
  private final long minEdgeWeight;// minimal weight in Path edges

  // Builder Pattern
  public static class PathQuerySpecBuilder {
    // Required fields.
    private final byte[] startNodeId;
    private final byte[] endNodeId;

    // Optional fields with default value.
    private RelationType requiredRelType = null;// null means all RelationTypes are acceptable.
    private Attribute requiredAttribute;// Default to the same as Start Node type
    private TimeRange timeRange = TimeRange.ANY_TIME;
    private int maxPathLength = 3;//TODO
    private long maxEdgeWeight = Integer.MAX_VALUE;
    private long minEdgeWeight = 1;

    public PathQuerySpecBuilder(byte[] startId, byte[] endId) {
      this.startNodeId = startId;
      this.endNodeId = endId;
      // Default to the same as Start Node type
      requiredAttribute = new Node(startId).getType().getAttribute();
    }

    public PathQuerySpecBuilder requiredAttribute(Attribute attribute) {
      this.requiredAttribute = attribute;
      return this;
    }

    public PathQuerySpecBuilder requiredRelType(RelationType type) {
      this.requiredRelType = type;
      return this;
    }

    public PathQuerySpecBuilder timeRange(TimeRange tr) {
      this.timeRange = tr;
      return this;
    }

    /**
     * Set maxPathLength, max value is inclusive. Path length will be in [1,maxPathLength]
     */
    public PathQuerySpecBuilder maxPathLength(int length) {
      this.maxPathLength = length;
      return this;
    }

    /**
     * Set maxEdgeCount, max value is inclusive. Edge count will be in [minEdgeWeight,maxEdgeWeight]
     */
    public PathQuerySpecBuilder maxEdgeCount(long maxCount) {
      this.maxEdgeWeight = maxCount;
      return this;
    }

    /**
     * Set minEdgeCount. Edge count will be in [minEdgeWeight,maxEdgeWeight]
     */
    public PathQuerySpecBuilder minEdgeCount(long minCount) {
      this.minEdgeWeight = minCount;
      return this;
    }

    public PathQuerySpec build() {
      return new PathQuerySpec(this);
    }

  }

  private PathQuerySpec(PathQuerySpecBuilder builder) {
    this.startNode = new Node(builder.startNodeId);
    this.endNode = new Node(builder.endNodeId);
    this.requiredAttribute = builder.requiredAttribute;
    this.requiredRelType = builder.requiredRelType;
    this.timeRange = builder.timeRange;
    this.maxPathLength = builder.maxPathLength;
    this.maxEdgeWeight = builder.maxEdgeWeight;
    this.minEdgeWeight = builder.minEdgeWeight;

    // Check fields
    if (requiredAttribute == null) throw new NullPointerException("nodeType=" + requiredAttribute);
    if (startNode.representSameNode(endNode))
      throw new IllegalArgumentException("Start and end Node can not be the same.");
    // check types. Currently the start, end and required Nodes in path MUST be the SAME type. //TODO 现在应该是不需要这个限制了.!
    /*if (!(startNode.getType() == endNode.getType() && startNode.getType() == requiredNodeType)) {
      // TODO remove type restriction
      throw new IllegalArgumentException(
          "Currently the start, end and required Nodes in path MUST be the SAME type. "
              + "startNode type=" + startNode.getType() + " endNode type=" + endNode.getType()
              + " requiredNodeType=" + requiredNodeType);
    }*/

    if (timeRange == null) throw new NullPointerException("null TimeRange");
    if (maxPathLength <= 0) throw new IllegalArgumentException("maxPathLength=" + maxPathLength);
    if (minEdgeWeight < 0 || maxEdgeWeight < 0 || minEdgeWeight > maxEdgeWeight)
      throw new IllegalArgumentException("minEdgeCount=" + minEdgeWeight + " maxEdgeCount="
          + maxEdgeWeight);
  }

  public Node getStartNode() {
    return startNode;
  }

  public Node getEndNode() {
    return endNode;
  }

  

  public Attribute getRequiredAttribute() {
	return requiredAttribute;
}

public RelationType getRequiredRelType() {
    return requiredRelType;
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }

  public int getMaxPathLength() {
    return maxPathLength;
  }

  public long getMaxEdgeWeight() {
    return maxEdgeWeight;
  }

  public long getMinEdgeWeight() {
    return minEdgeWeight;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[PathQuerySpec] ");
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
