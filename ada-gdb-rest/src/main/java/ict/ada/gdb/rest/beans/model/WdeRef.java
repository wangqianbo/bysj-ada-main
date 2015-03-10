package ict.ada.gdb.rest.beans.model;

import ict.ada.gdb.rest.util.NodeIdConveter;
import ict.ada.gdb.rest.util.WDEIdConverter;

public class WdeRef {

  private String wdeid;
  private int offset;
  private int length;
  private String type;

  public WdeRef() {
  }

  public WdeRef(ict.ada.common.model.WdeRef wderef) {
    this.wdeid = NodeIdConveter.toString(wderef.getWdeId());
    this.offset = wderef.getOffset();
    this.length = wderef.getLength();
    this.type = WDEIdConverter.getChannel(wderef.getWdeId());
  }

  /**
   * @return the wdeid
   */
  public String getWdeid() {
    return wdeid;
  }

  /**
   * @param wdeid
   *          the wdeid to set
   */
  public void setWdeid(String wdeid) {
    this.wdeid = wdeid;
  }

  /**
   * @return the offset
   */
  public int getOffset() {
    return offset;
  }

  /**
   * @param offset
   *          the offset to set
   */
  public void setOffset(int offset) {
    this.offset = offset;
  }

  /**
   * @return the length
   */
  public int getLength() {
    return length;
  }

  /**
   * @param length
   *          the length to set
   */
  public void setLength(int length) {
    this.length = length;
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

  @Override
  public boolean equals(Object bean) {
    if (bean == null) return false;
    if (bean == this) return true;
    if (bean.getClass() != this.getClass()) return false;
    WdeRef wderefBean = (WdeRef) bean;
    if (wderefBean.getWdeid().equals(this.getWdeid()) && wderefBean.getLength() == this.getLength()
        && wderefBean.getOffset() == this.getOffset()) return true;
    else return false;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    hash = hash * 17 + this.getLength();
    hash = hash * 31 + this.getWdeid().hashCode();
    hash = hash * 13 + this.getOffset();
    return hash;
  }

}
