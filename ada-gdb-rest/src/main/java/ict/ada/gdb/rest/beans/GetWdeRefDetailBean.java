package ict.ada.gdb.rest.beans;

import java.util.List;

public class GetWdeRefDetailBean {
  private String errorCode = "success";
  private List<WDEDetailBean> wderefs;
  private int total;
  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public List<WDEDetailBean> getWderefs() {
    return wderefs;
  }

  public void setWderefs(List<WDEDetailBean> wderefs) {
    this.wderefs = wderefs;
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }

}
