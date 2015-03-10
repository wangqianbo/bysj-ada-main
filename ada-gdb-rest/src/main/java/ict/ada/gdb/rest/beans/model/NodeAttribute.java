package ict.ada.gdb.rest.beans.model;

import java.util.List;

import org.codehaus.jackson.map.annotate.JsonSerialize;

public class NodeAttribute {

  private String key;
  private String value;
  private int count;
  private List<WdeRef> wdeRefs;

  public NodeAttribute() {
    super();
  }

  /**
   * @return the key
   */
  public String getKey() {
    return key;
  }

  /**
   * @param key
   *          the key to set
   */
  public void setKey(String key) {
    this.key = key;
  }

  /**
   * @return the value
   */
  public String getValue() {
    return value;
  }

  /**
   * @param value
   *          the value to set
   */
  public void setValue(String value) {
    this.value = value;
  }

  /**
   * @return the count
   */
  public int getCount() {
    return count;
  }

  /**
   * @param count
   *          the count to set
   */
  public void setCount(int count) {
    this.count = count;
  }

  public void setWdeRefs(List<WdeRef> wdeRefs) {
    this.wdeRefs = wdeRefs;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  public List<WdeRef> getWdeRefs() {
    return wdeRefs;
  }

}
