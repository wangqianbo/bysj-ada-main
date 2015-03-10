package ict.ada.gdb.rest.beans;

import java.util.ArrayList;
import java.util.List;

public class QueryWdeRefDetailsBean {
  private List<WdeRef> details;

  public QueryWdeRefDetailsBean() {
    details = new ArrayList<WdeRef>();
  }

  public List<WdeRef> getDetails() {
    return details;
  }

  public void setDetails(List<WdeRef> details) {
    this.details = details;
  }

  public static class WdeRef {
    private String id;
    private int off;
    private int len;

    public String getId() {
      return id;
    }

    public void setId(String id) {

      this.id = id;
    }

    public int getOff() {
      return off;
    }

    public void setOff(int off) {
      this.off = off;
    }

    public int getLen() {
      return len;
    }

    public void setLen(int len) {
      this.len = len;
    }

  }
}
