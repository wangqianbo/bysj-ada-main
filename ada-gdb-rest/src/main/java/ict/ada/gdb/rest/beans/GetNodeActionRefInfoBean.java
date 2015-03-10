package ict.ada.gdb.rest.beans;

import ict.ada.common.model.NodeAttribute;
import ict.ada.common.util.Pair;
import ict.ada.gdb.rest.beans.model.WdeRef;

import java.util.ArrayList;
import java.util.List;

public class GetNodeActionRefInfoBean {

	  private String errorCode = "success";
	  private List<ActionTimeLine> result;
	  private int len;

	  public GetNodeActionRefInfoBean(
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
		  private List<WdeRef> wdeRefs;
	    private int ts;

	    public ActionTimeLine(int ts, NodeAttribute attr) {
	    	wdeRefs = new ArrayList<WdeRef>();
	    	for (ict.ada.common.model.WdeRef wdeRef : attr.getValues().get(0).getWdeRefs()) {
	 	        wdeRefs.add(new WdeRef(wdeRef));
	 	      }
	      this.ts = ts;
	    }

	    

	    public List<WdeRef> getWdeRefs() {
			return wdeRefs;
		}



		public int getTs() {
	      return ts;
	    }

	  }



}
