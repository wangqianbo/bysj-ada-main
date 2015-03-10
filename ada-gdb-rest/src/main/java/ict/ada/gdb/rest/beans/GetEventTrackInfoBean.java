package ict.ada.gdb.rest.beans;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.util.StringUtils;

public class GetEventTrackInfoBean {
  private List<Event> datas;
  private String errorCode = "success";

  public GetEventTrackInfoBean() {
    datas = new ArrayList<Event>();
  }

  public void addEvent(int id, int type, String title, String group, String date, String ab,
      ict.ada.common.model.WdeRef wdeRef) {
    datas.add(new Event(id, type, title, group, date, ab, wdeRef));
  }

  public List<Event> getDatas() {
    return datas;
  }

  public void setDatas(List<Event> datas) {
    this.datas = datas;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public static class Event {
    private int id;
    private int type;
    private String title;
    private String group;
    private String date;
    private String ab;
    private WdeRef wdeRef;

    public Event(int id, int type, String title, String group, String date, String ab,
        ict.ada.common.model.WdeRef wdeRef) {
      this.id = id;
      this.type = type;
      this.title = title;
      this.group = group;
      this.date = date;
      this.ab = ab;
      this.wdeRef = new WdeRef(wdeRef);
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public int getType() {
      return type;
    }

    public void setType(int type) {
      this.type = type;
    }

    public String getTitle() {
      return title;
    }

    public void setTitle(String title) {
      this.title = title;
    }

    public String getGroup() {
      return group;
    }

    public void setGroup(String group) {
      this.group = group;
    }

    public String getDate() {
      return date;
    }

    public void setDate(String date) {
      this.date = date;
    }

    public String getAb() {
      return ab;
    }

    public void setAb(String ab) {
      this.ab = ab;
    }

    public WdeRef getWdeRef() {
      return wdeRef;
    }

    public void setWdeRef(WdeRef wdeRef) {
      this.wdeRef = wdeRef;
    }

  }

  public static class WdeRef {
    private String id;
    private int len;
    private int offset;

    public WdeRef(ict.ada.common.model.WdeRef wdeRef) {
      this.id = StringUtils.byteToHexString(wdeRef.getWdeId());
      len = wdeRef.getLength();
      offset = wdeRef.getOffset();
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public int getLen() {
      return len;
    }

    public void setLen(int len) {
      this.len = len;
    }

    public int getOffset() {
      return offset;
    }

    public void setOffset(int offset) {
      this.offset = offset;
    }

  }
}
