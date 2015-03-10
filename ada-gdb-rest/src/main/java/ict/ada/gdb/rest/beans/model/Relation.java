package ict.ada.gdb.rest.beans.model;

public class Relation {

  private String type = null;
  private int count;

  public Relation(ict.ada.common.model.Relation relation) {
    this.type = relation.getType().toString();
    this.count = relation.getWeight();
  }

  public String getType() {
    return type;
  }

  public int getCount() {
    return count;
  }

}
