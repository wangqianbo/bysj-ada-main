package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Event;
import ict.ada.gdb.rest.dao.bean.AdaEventBean;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnore;

public class GetEventNodeNameBean {
  private String errorCode = "success";
  private String type = "event";
  private int count;
  private List<Event> nodeList;

  public GetEventNodeNameBean(List<AdaEventBean> adaEvents) {
    nodeList = new ArrayList<Event>();
    for (AdaEventBean bean : adaEvents)
      nodeList.add(new Event(bean));
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public String getType() {
    return type;
  }

  public int getCount() {
    return count;
  }

  public List<Event> getNodeList() {
    return nodeList;
  }
}
