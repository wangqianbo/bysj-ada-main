package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Event;
import ict.ada.gdb.rest.dao.bean.AdaEventBean;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.rest.util.EventUtil;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public class GetReleventEventsBean {
  private String errorCode = "success";
  private List<SimilarEvent> results;

  public GetReleventEventsBean(int len) {
    results = new ArrayList<SimilarEvent>(len);
  }

  public void addEvent(AdaEventBean bean, List<ict.ada.common.model.Node> elements, int score) {
    results.add(new SimilarEvent(bean, elements, score));
  }

  public String getErrorCode() {
    return errorCode;
  }

  public List<SimilarEvent> getResults() {
    return results;
  }

  public static class SimilarEvent {
    private int score;
    private Event eventInfo;
    private List<Element> elements;

    public SimilarEvent(AdaEventBean bean, List<ict.ada.common.model.Node> elements, int score) {
      this.eventInfo = new Event(bean);
      this.elements = new ArrayList<Element>();
      for (ict.ada.common.model.Node node : elements)
        this.elements.add(new Element(node));
      this.score = score;
    }

    public int getScore() {
      return score;
    }

    @JsonProperty("event_info")
    public Event getEventInfo() {
      return eventInfo;
    }

    public List<Element> getElements() {
      return elements;
    }

  }

  public static class Element {
    private String name;
    private String type;
    private List<String> sname;

    public Element(String name, String type, List<String> sname) {
      this.name = name;
      this.type = type;
      this.sname = sname;
    }

    public Element(ict.ada.common.model.Node node) {
      this.name = node.getName();
      this.type = NodeTypeMapper.getAttributeName(node.getType().getAttribute());
      this.sname = node.getSnames();
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    public List<String> getSname() {
      return sname;
    }

  }
}
