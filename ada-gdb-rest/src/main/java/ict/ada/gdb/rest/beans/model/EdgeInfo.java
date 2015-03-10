package ict.ada.gdb.rest.beans.model;

import ict.ada.common.model.Relation;
import ict.ada.gdb.util.NodeIdConveter;

public class EdgeInfo {

  private String type;
  private int weight;
  private String relation_id;

  public EdgeInfo() {
  }

  public EdgeInfo(Relation relation) {
    this.type = relation.getType().getStringForm();
    this.weight = relation.getWeight();
    this.relation_id = NodeIdConveter.toString(relation.getId());
  }

  /**
   * @return the type
   */
  public String getType() {
    return type;
  }

  /**
   * @param type
   *          the type to set
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * @return the weight
   */
  public int getWeight() {
    return weight;
  }

  /**
   * @param weight
   *          the weight to set
   */
  public void setWeight(int weight) {
    this.weight = weight;
  }

  /**
   * @return the relation_id
   */
  public String getRelation_id() {
    return relation_id;
  }

  /**
   * @param relation_id
   *          the relation_id to set
   */
  public void setRelation_id(String relation_id) {
    this.relation_id = relation_id;
  }

}
