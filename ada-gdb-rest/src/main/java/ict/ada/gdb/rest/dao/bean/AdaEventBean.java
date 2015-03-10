package ict.ada.gdb.rest.dao.bean;

import java.util.Date;
import java.util.List;

public class AdaEventBean {
  private int _id;
  private String ab;
  private Date it;
  private String kv;
  private int s;
  private String t;
  private List<String> tags;
  private Date ut;
  private int ch;// 来源通道 1:新闻，2:论坛，3：百科，4，微博
  private String ele;// 事件要素

  public int get_id() {
    return _id;
  }

  public void set_id(int _id) {
    this._id = _id;
  }

  public String getAb() {
    return ab;
  }

  public void setAb(String ab) {
    this.ab = ab;
  }

  public Date getIt() {
    return it;
  }

  public void setIt(Date it) {
    this.it = it;
  }

  public String getKv() {
    return kv;
  }

  public void setKv(String kv) {
    this.kv = kv;
  }

  public int getS() {
    return s;
  }

  public void setS(int s) {
    this.s = s;
  }

  public String getT() {
    return t;
  }

  public void setT(String t) {
    this.t = t;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public Date getUt() {
    return ut;
  }

  public void setUt(Date ut) {
    this.ut = ut;
  }

  public int getCh() {
    return ch;
  }

  public void setCh(int ch) {
    this.ch = ch;
  }

  public String getEle() {
    return ele;
  }

  public void setEle(String ele) {
    this.ele = ele;
  }

  @Deprecated
  public String getMethod() {
    switch (s) {
    case 1:
      return "规则过滤";
    case 2:
      return "聚类";
    case 3:
      return "百科";
    default:
      return null;
    }

  }

  @Deprecated
  public String getChannel() {
    switch (ch) {
    case 1:
      return "新闻";
    case 2:
      return "论坛";
    case 3:
      return "百科";
    case 4:
      return "微博";
    default:
      return null;
    }
  }
}
