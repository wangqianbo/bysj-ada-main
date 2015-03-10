package ict.ada.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * An attribute of a Node.<br>
 * One attribute has one key(e.g. age) and may have one or more values(e.g. 16-years-old,
 * 20-years-old). Each value is associated with a few WdeRefs indicating where the value is
 * extracted.
 */
public class NodeAttribute {

  private String key;
  private List<AttrValueInfo> values;

  public NodeAttribute(String key) {
    this(key, new ArrayList<AttrValueInfo>());
  }

  public NodeAttribute(String key, List<AttrValueInfo> values) {
    if (key == null) throw new NullPointerException("null key");
    if (values == null) throw new NullPointerException("null AttrValueInfo list");
    this.key = key;
    this.values = values;
  }

  public void addAttrValueInfo(AttrValueInfo valueInfo) {
    values.add(valueInfo);
  }

  public String getKey() {
    return key;
  }

  public List<AttrValueInfo> getValues() {
    return values;
  }

  /**
   * Value and WdeRefs for a NodeAttribute
   */
  public static class AttrValueInfo {
    String value;
    List<WdeRef> wdeRefs;
    int wdeRefCount;

    public AttrValueInfo(String value, List<WdeRef> wdeRefs) {
      // TODO Can wdeRefs be NULL?
      // || wdeRefs == null || wdeRefs.size() == 0
      if (value == null) {
        throw new IllegalArgumentException("value=" + value + " wdeRefs=" + wdeRefs);
      }
      this.value = value;
      this.wdeRefs = wdeRefs;
      if(wdeRefs==null||wdeRefs.size()==0)
    	  this.wdeRefCount=0;
      else 
          this.wdeRefCount=wdeRefs.size();
    }

    /**
     * Used when only WdeRefs' count is retrieved.
     */
    public AttrValueInfo(String value, int wdeRefCount) {
      this(value, null);
      if (wdeRefCount < 0) throw new IllegalArgumentException("wdeRefCount=" + wdeRefCount);
      this.wdeRefCount = wdeRefCount;
    }

    public String getValue() {
      return value;
    }

    public List<WdeRef> getWdeRefs() {
      return wdeRefs;
    }

    public int getWdeRefCount() {
      return wdeRefCount;
    }

  }
}
