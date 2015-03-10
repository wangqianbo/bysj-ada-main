package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.WdeRef;
import ict.ada.gdb.rest.util.NodeIdConveter;
import ict.ada.gdb.rest.util.WDEIdConverter;

import java.util.ArrayList;
import java.util.List;

public class GetWdeRefsByIdBean {
  private String errorCode = "success";
  private List<WdeRef> wderefs;
  private int total;

  public GetWdeRefsByIdBean() {
    wderefs = new ArrayList<WdeRef>();
  }

  public GetWdeRefsByIdBean(ict.ada.common.model.Node node) {
    wderefs = new ArrayList<WdeRef>();
    for (ict.ada.common.model.WdeRef wderef : node.getWdeRefs()) {
      wderefs.add(new WdeRef(wderef));
    }
  }

  public void addWdeRef(ict.ada.common.model.WdeRef wderef) {
    wderefs.add(new WdeRef(wderef));
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public List<WdeRef> getWderefs() {
    return wderefs;
  }

  public void setWderefs(List<WdeRef> wderefs) {
    this.wderefs = wderefs;
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
  }

}
