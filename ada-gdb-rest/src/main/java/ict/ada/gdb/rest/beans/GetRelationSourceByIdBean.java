package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.WdeRef;

import java.util.ArrayList;
import java.util.List;

public class GetRelationSourceByIdBean {
  private String errorCode;
  private List<WdeRef> wderefs;

  public GetRelationSourceByIdBean() {
    errorCode = "success";
    wderefs = new ArrayList<WdeRef>();
  }

  public void addWderef(WdeRef wderef) {

    wderefs.add(wderef);
  }

  /**
   * @return the errorCode
   */
  public String getErrorCode() {
    return errorCode;
  }

  /**
   * @param errorCode
   *          the errorCode to set
   */
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  /**
   * @return the wderefs
   */
  public List<WdeRef> getWderefs() {
    return wderefs;
  }

  /**
   * @param wderefs
   *          the wderefs to set
   */
  public void setWderefs(List<WdeRef> wderefs) {
    this.wderefs = wderefs;
  }
}
