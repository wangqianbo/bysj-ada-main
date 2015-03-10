package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.ScanRelationInferBean.Rule.Regulation;

import java.util.ArrayList;
import java.util.List;

public class OpenioRuleBean1 {
  private List<Regulation> rule = null;

  public OpenioRuleBean1() {
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
