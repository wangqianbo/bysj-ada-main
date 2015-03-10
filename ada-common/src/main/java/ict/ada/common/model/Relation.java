package ict.ada.common.model;

import ict.ada.common.util.BytesTool;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Relation {
  //public static final int RELATIONID_SIZE = Edge.EDGEID_SIZE + RelationType.RELATIONTYPE_BYTES_SIZE;

  /** Relation id: |--Edge Id--|--relation type bytes--| */
  private byte[] id;

  private Edge parentEdge;
  private RelationType type;
  private int weight;
  private List<WdeRef> wdeRefs;

  public Relation(Edge parent, RelationType type) {
    if (parent == null || type == null)
      throw new NullPointerException("parent =" + parent + " type=" + type);
    this.parentEdge = parent;
    this.type = type;
    this.id = null;
  }

  public Relation(byte[] parentEdgeId, RelationType relType) {
    Edge.checkEdgeId(parentEdgeId);
    if (relType == null) throw new NullPointerException("null relType");
    this.type = relType;
    this.id = BytesTool.add(parentEdgeId, relType.getBytesForm());
  }

  public Relation(byte[] relationId) {
    checkRelationId(relationId);
    this.id = relationId;
    try {
		this.type = RelationType.getType(new String(relationId,Edge.EDGEID_SIZE,relationId.length-Edge.EDGEID_SIZE,"UTF-8"));
	} catch (UnsupportedEncodingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
 
  /**
   * 仅仅检查EdgeId
 * @param relId
 */
public static void checkRelationId(byte[] relId) {
    Edge.checkEdgeId(Arrays.copyOfRange(relId, 0, Edge.EDGEID_SIZE));
  }

  public Edge getParentEdge() {
    return parentEdge;
  }

  public byte[] getId() {
    if (id != null) {
      return id;
    } else if (parentEdge != null && parentEdge.getId() != null && type != null) {
      id = BytesTool.add(parentEdge.getId(), type.getBytesForm());
      return id;
    } else {
      throw new IllegalStateException("No Relation id data available.");
    }
  }

  public NodeType getParentEdgeHeadNodeType() {
    if (parentEdge != null) {
      return parentEdge.getHeadNodeType();
    } else if (id != null) {
      return NodeType.getType( id[0], id[1] );// head Node type is in the first 2
                                                           // bytes.
    } else {
      throw new IllegalStateException("No parent Edge head Node type info available.");
    }
  }

  public NodeType getParentEdgeTailNodeType() {
    if (parentEdge != null) {
      return parentEdge.getTailNodeType();
    } else if (id != null) {
      // tail Node type is in the 2 bytes below
      return NodeType.getType( id[0 + Node.NODEID_SIZE], id[1 + Node.NODEID_SIZE] );
    } else {
      throw new IllegalStateException("No parent Edge head Node type info available.");
    }
  }

  public void setType(RelationType type) {
    this.type = type;
  }

  public int getWeight() {
    return weight;
  }

  public void setWeight(int weight) {
    this.weight = weight;
  }

  /**
   * never return null
   * @return
   */
  public List<WdeRef> getWdeRefs() {
    if (wdeRefs == null) {
      return Collections.emptyList();
    } else {
      return wdeRefs;
    }
  }

  public void setWdeRefs(List<WdeRef> wdeRefs) {
    this.wdeRefs = wdeRefs;
  }

  public RelationType getType() {
    return type;
  }
  
  @Override
  public String toString() {
    return "Rel{ t=" + type + " w=" + weight + "}";
  }

}
