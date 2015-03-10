package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Event;
import ict.ada.gdb.rest.common.ReleventEventBean;
import ict.ada.gdb.rest.dao.bean.AdaEventBean;
import ict.ada.gdb.rest.util.EventUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonProperty;
import org.javatuples.Triplet;

public class GetReleventEventsMBean {
  private String errorCode = "success";
  private List<SimilarEvent> results;

  public GetReleventEventsMBean(List<ReleventEventBean> beans) {

    System.out.println(beans.size());

    results = new ArrayList<SimilarEvent>(beans.size());
    for (ReleventEventBean result : beans) {
      if (result != null) results.add(new SimilarEvent(result.getEventBean(),
          result.getShareTags(), result.getScore()));
    }
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
    private List<String> shareTags;

    public SimilarEvent(AdaEventBean bean, Collection<String> shareTags, double score) {
      this.eventInfo = new Event(bean);
      this.shareTags = new ArrayList<String>(shareTags);
      this.score = (int) (score * 100);
    }

    public int getScore() {
      return score;
    }

    @JsonProperty("event_info")
    public Event getEventInfo() {
      return eventInfo;
    }

    @JsonProperty("tags")
    public List<String> getShareTags() {
      return shareTags;
    }

  }
}
