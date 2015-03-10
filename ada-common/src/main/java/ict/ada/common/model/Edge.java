package ict.ada.common.model;

import ict.ada.common.util.BytesTool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A directed edge. An Edge contains multiple Relations.<br>
 * This class is NOT thread-safe.
 */
public class Edge {
  public static int EDGEID_SIZE = 2 * Node.NODEID_SIZE;
  /** Edge id is "|--head id--|--tail id--|" */
  private byte[] id;

  private Node head;
  private Node tail;
  private List<Relation> relations;
  private int edgeWeight;
  public Edge(Node head, Node tail) {
    if (head == null || tail == null)
      throw new NullPointerException("head=" + head + " tail=" + tail);
    this.head = head;
    this.tail = tail;
    edgeWeight=1;
  }

  public void addRelation(Relation rel) {
    if (rel.getParentEdge() != this)
      throw new IllegalStateException("The Relation to add does not belong to this Edge.");
    if (relations == null) {
      relations = new ArrayList<Relation>();
    }
    relations.add(rel);
  }

  public static void checkEdgeId(byte[] edgeId) {
    Node.checkNodeId(Arrays.copyOfRange(edgeId, 0, Node.NODEID_SIZE));
    Node.checkNodeId(Arrays.copyOfRange(edgeId, Node.NODEID_SIZE, 2 * Node.NODEID_SIZE));
  }

  /**
   * Add one relation.
   * 
   * @param type
   * @param weight
   * @param wdeRefs
   *          for relations without WdeRefs, this can be NULL
   */
  public void addRelation(RelationType type, int weight, List<WdeRef> wdeRefs) {
    Relation rel = new Relation(this, type);
    rel.setWeight(weight);
    if (wdeRefs != null) {// Some relation may not contain wdeRefs
      rel.setWdeRefs(wdeRefs);
    }
    this.addRelation(rel);
  }

  public void addRelation(RelationType type, int weight) {
    this.addRelation(type, weight, null);
  }

  /**
   * @return never return null
   */
  public List<Relation> getRelations() {
    if (relations == null) {
      return Collections.emptyList();
    } else {
      return relations;
    }
  }

  public int getEdgeWeight() {
    if (relations == null) {
      return edgeWeight;
    } else {// TODO define edge relation
      int total = 0;
      for (Relation rel : relations) {
        total += rel.getWeight();
      }
      return total;
    }
  }
public void addWeight(int weight){
  this.edgeWeight+=weight;
}
  public void setEdgeWeight(int edgeWeight) {
	this.edgeWeight = edgeWeight;
}

public Node getHead() {
    return head;
  }

  public Node getTail() {
    return tail;
  }

  /**
   * Edge id is "|--head id--|--tail id--|".<br>
   * If id is absent, it will be calculated by two Node info.<br>
   * NOTE: Not thread-safe.
   * 
   * @return
   */
  public byte[] getId() {
    if (id != null) {
      return id;
    } else if (head.getId() != null && tail.getId() != null) {
      id = BytesTool.add(head.getId(), tail.getId());
      return id;
    } else {
      throw new IllegalStateException("Head and tail Nodes' id are not ready.");
    }
  }

  /**
   * Get head Node's type of this Edge.<br>
   * Type info can be found in Edge id or head Node object.
   * 
   * @throws IllegalStateException
   *           if no type info is available
   * @return
   */
  public NodeType getHeadNodeType() {
    if (head != null && head.getType() != null) {
      return head.getType();
    } else if (id != null) {
      return NodeType.getType(id[0], id[1] );
    } else {
      throw new IllegalStateException("No head Node type info available.");
    }
  }

  /**
   * Get tail Node's type of this Edge.<br>
   * Type info can be found in Edge id or tail Node object.
   * 
   * @throws IllegalStateException
   *           if no type info is available
   * @return
   */

  public NodeType getTailNodeType() {
    if (tail != null && tail.getType() != null) {
      return tail.getType();
    } else if (id != null) {
      return NodeType.getType(id[0 + Node.NODEID_SIZE], id[1 + Node.NODEID_SIZE] );
    } else {
      throw new IllegalStateException("No tail Node type info available.");
    }
  }

  /**
   * Tell if head and tail are identical nodes.
   */
  public boolean identicalHeadAndTail() {
    return this.head.representSameNode(this.tail);
  }

  @Override
  public String toString() {
    return "Edge{ h=" + head + " t=" + tail + " rels=" + relations + "}";
  }
}
