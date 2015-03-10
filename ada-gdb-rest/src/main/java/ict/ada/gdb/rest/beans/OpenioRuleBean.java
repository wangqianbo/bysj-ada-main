package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.GetRelationInferBean.Rule.Regulation;

import java.util.ArrayList;
import java.util.List;

public class OpenioRuleBean {
  private List<Regulation> rule = null;

  public OpenioRuleBean() {
    rule = new ArrayList<Regulation>();
  }

  /**
   * @return the rule
   */
  public List<Regulation> getRule() {
    return rule;
  }

  /**
   * @param rule
   *          the rule to set
   */
  public void setRule(List<Regulation> rule) {
    this.rule = rule;
  }

}
