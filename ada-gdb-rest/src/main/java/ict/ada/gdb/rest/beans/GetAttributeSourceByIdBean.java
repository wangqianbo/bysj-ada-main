package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.WdeRef;

import java.util.ArrayList;
import java.util.List;

public class GetAttributeSourceByIdBean {
  private String errorCode = "success";
  private List<WdeRef> wderefs;

  public GetAttributeSourceByIdBean() {
    super();
  }

  public void initWderefs() {
    wderefs = new ArrayList<WdeRef>();
  }

  public void appendWderefs(WdeRef wdeRef) {
    wderefs.add(wdeRef);
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
