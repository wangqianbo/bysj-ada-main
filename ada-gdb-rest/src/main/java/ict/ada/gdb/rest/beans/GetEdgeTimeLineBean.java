package ict.ada.gdb.rest.beans;

import ict.ada.common.util.Pair;
import ict.ada.gdb.rest.beans.model.Relation;

import java.util.ArrayList;
import java.util.List;

public class GetEdgeTimeLineBean {

  private String errorCode = "success";
  private List<TimeLine> timeline;

  public GetEdgeTimeLineBean(List<Pair<Integer, List<ict.ada.common.model.Relation>>> results) {
    this.timeline = new ArrayList<TimeLine>(results.size());
    for (Pair<Integer, List<ict.ada.common.model.Relation>> result : results)
      timeline.add(new TimeLine(result));
  }

  public static class TimeLine {
    private int timestamp;
    private List<Relation> infos;

    public TimeLine(Pair<Integer, List<ict.ada.common.model.Relation>> result) {
      this.timestamp = result.getFirst();
      infos = new ArrayList<Relation>(result.getSecond().size());
      for (ict.ada.common.model.Relation relation : result.getSecond())
        infos.add(new Relation(relation));
    }

    public int getTimestamp() {
      return timestamp;
    }

    public List<Relation> getInfos() {
      return infos;
    }

  }

  public String getErrorCode() {
    return errorCode;
  }

  public List<TimeLine> getTimeline() {
    return timeline;
  }

}
