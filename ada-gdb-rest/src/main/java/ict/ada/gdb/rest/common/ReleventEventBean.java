package ict.ada.gdb.rest.common;

import ict.ada.gdb.rest.dao.bean.AdaEventBean;

import java.util.Set;

public class ReleventEventBean {
  private AdaEventBean eventBean;
  private Set<String> shareTags;
  private double score;

  public ReleventEventBean() {
    this.eventBean = null;
    this.shareTags = null;
    this.score = 0;
  }

  public ReleventEventBean(AdaEventBean eventBean, Set<String> shareTags, double score) {
    this.eventBean = eventBean;
    this.shareTags = shareTags;
    this.score = score;
  }

  public AdaEventBean getEventBean() {
    return eventBean;
  }

  public Set<String> getShareTags() {
    return shareTags;
  }

  public double getScore() {
    return score;
  }

}
