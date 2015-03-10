package ict.ada.gdb.rest.beans.model;

import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.annotate.JsonProperty;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.gdb.rest.dao.bean.AdaEventBean;
import ict.ada.gdb.rest.util.EventUtil;

public class Event {

  private String name;
  private String id;
  private String ab;
  private String channel;
  private String method;
  private long insert_time;

  public Event(AdaEventBean bean) {
    this.id = StringUtils.byteToHexString(NodeType.getType(EventUtil.getChannel(bean.getCh()),
        Attribute.EVENT).getBytesForm())
        + bean.get_id();
    this.name = bean.getT();
    this.ab = bean.getAb();
    if (this.ab == null || this.ab.equals("")) this.ab = null;
    this.method = EventUtil.getMethodName(bean.getS());
    if (this.method == null || this.method.equals("")) this.method = null;
    this.channel = EventUtil.getChannelName(bean.getCh());
    this.insert_time = bean.getIt().getTime() / 1000;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAb() {
    return ab;
  }

  public void setAb(String ab) {
    this.ab = ab;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getChannel() {
    return channel;
  }

  public void setChannel(String channel) {
    this.channel = channel;
  }

  public String getMethod() {
    return method;
  }

  public void setMethod(String method) {
    this.method = method;
  }

  public long getInsert_time() {
    return insert_time;
  }

  public void setInsert_time(long insert_time) {
    this.insert_time = insert_time;
  }

}
