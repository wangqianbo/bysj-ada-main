package ict.ada.gdb.rest.beans;

import java.util.ArrayList;
import java.util.List;

import ict.ada.common.model.NodeAttribute;
import ict.ada.common.model.NodeAttribute.AttrValueInfo;
import ict.ada.common.util.Pair;
import ict.ada.gdb.rest.beans.model.WdeRef;

public class GetNodeActionInfoBean {
  private String errorCode = "success";
  private List<ActionTimeLine> result;
  private int len;

  public GetNodeActionInfoBean(
      List<Pair<Integer, ict.ada.common.model.NodeAttribute>> tsAttributePairs) {
    result = new ArrayList<ActionTimeLine>();
    if (tsAttributePairs != null && tsAttributePairs.size() != 0) {
      for (Pair<Integer, NodeAttribute> tsAttributePair : tsAttributePairs) {
        result.add(new ActionTimeLine(tsAttributePair.getFirst(), tsAttributePair.getSecond()));
      }
    }
    this.len = result.size();
  }

  public String getErrorCode() {
    return errorCode;
  }

  public List<ActionTimeLine> getResult() {
    return result;
  }

  public int getLen() {
    return len;
  }

  public static class ActionTimeLine {
    private List<Action> actions;
    private int ts;

    public ActionTimeLine(int ts, NodeAttribute attr) {
      actions = new ArrayList<Action>();
      for (AttrValueInfo value : attr.getValues()) {
        actions.add(new Action(value));
      }
      this.ts = ts;
    }

    public List<Action> getActions() {
      return actions;
    }

    public int getTs() {
      return ts;
    }

  }

  public static class Action {
    private String action;
    private int count;
   // private List<WdeRef> wdeRefs;

    public Action(AttrValueInfo value) {
      this.action = value.getValue();
      //wdeRefs = new ArrayList<WdeRef>();
      //for (ict.ada.common.model.WdeRef wdeRef : value.getWdeRefs()) {
       // wdeRefs.add(new WdeRef(wdeRef));
      //}
      this.count =  value.getWdeRefs().size();
    }

    public String getAction() {
      return action;
    }

    public int getCount() {
      return count;
    }
  }
}
