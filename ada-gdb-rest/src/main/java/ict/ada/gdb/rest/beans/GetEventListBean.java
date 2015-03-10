package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Event;
import ict.ada.gdb.rest.dao.bean.AdaEventBean;
import ict.ada.gdb.rest.util.EventUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class GetEventListBean {

  private String errorCode = "success";
  private List<NodeList> results;

  public GetEventListBean() {
    results = new ArrayList<NodeList>();
  }

  public GetEventListBean(List<AdaEventBean> adaEvents, int channelIntType, int methodIntType,
      int count) {
    // HashMap<Integer,List<AdaEventBean>> map=new HashMap<Integer,List<AdaEventBean>>();
    results = new ArrayList<NodeList>();
    if (channelIntType != -1) results.add(new NodeList(EventUtil
        .getChannelStringType(channelIntType), adaEvents, count));
    else results.add(new NodeList(EventUtil.getMethodStringType(methodIntType), adaEvents, count));

    /*
     * if(_calss.equals("channel")){} else{ for(AdaEventBean bean : adaEvents){
     * if(!map.containsKey(bean.getS())) map.put(bean.getS(), new ArrayList<AdaEventBean>());
     * map.get(bean.getS()).add(bean); }
     * 
     * for(List<AdaEventBean> eventsList:map.values()){ results.add(new
     * NodeList(eventsList.get(0).getMethod(),eventsList)); }
     * 
     * }
     */
  }

  public void addResult(List<AdaEventBean> adaEvents, int channelIntType, int methodIntType,
      int count) {

    // HashMap<Integer,List<AdaEventBean>> map=new HashMap<Integer,List<AdaEventBean>>();
    // / results=new ArrayList<NodeList>();
    if (channelIntType != -1) results.add(new NodeList(EventUtil
        .getChannelStringType(channelIntType), adaEvents, count));
    else results.add(new NodeList(EventUtil.getMethodStringType(methodIntType), adaEvents, count));

    /*
     * if(_calss.equals("channel")){} else{ for(AdaEventBean bean : adaEvents){
     * if(!map.containsKey(bean.getS())) map.put(bean.getS(), new ArrayList<AdaEventBean>());
     * map.get(bean.getS()).add(bean); }
     * 
     * for(List<AdaEventBean> eventsList:map.values()){ results.add(new
     * NodeList(eventsList.get(0).getMethod(),eventsList)); }
     * 
     * }
     */

  }

  public String getErrorCode() {
    return errorCode;
  }

  public List<NodeList> getResults() {
    return results;
  }

  public static class NodeList {

    private String className;
    private int count;
    private List<Event> eventList;

    public NodeList(String className, List<AdaEventBean> events, int count) {
      this.className = className;
      this.eventList = new ArrayList<Event>();
      for (AdaEventBean bean : events)
        eventList.add(new Event(bean));
      this.count = count;

    }

    public String getClassName() {
      return className;
    }

    public void setClassName(String className) {
      this.className = className;
    }

    public List<Event> getEventList() {
      return eventList;
    }

    public void setEventList(List<Event> eventList) {
      this.eventList = eventList;
    }

    public int getCount() {
      return count;
    }

    public void setCount(int count) {
      this.count = count;
    }

  }

  public static class Node {
  }

}
