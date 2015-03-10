package ict.ada.common.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class Event {

	  /**
	   * Event id size in bytes<br>
	   * Event id: 32bits(int)
	   */
	  public static final int EVENTID_SIZE = 4;
	  private List<WdeRef> wdeRefs;
	 
	  private int id;
	  //just ignore below ,they are not defined in the GDB (in mongo)
	  private String ab;
	  private Date it;
	  private String kv;
	  private int s;
	  private String t;
	  private List<String>tags;
	  private Date ut;
	  public Event(int id) {
	    this.id=id;
	  }
	 
	  public void addWdeRef(WdeRef ref) {
	    if (wdeRefs == null) {
	      wdeRefs = new ArrayList<WdeRef>();
	    }
	    wdeRefs.add(ref);
	  }

	  public void addWdeRefs(List<WdeRef> refs) {
	    if (wdeRefs == null) {
	      wdeRefs = new ArrayList<WdeRef>();
	    }
	    for (WdeRef ref : refs) {
	      wdeRefs.add(ref);
	    }
	  }

	  public List<WdeRef> getWdeRefs() {
	    if (wdeRefs == null) return Collections.emptyList();
	    else return wdeRefs;
	  }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String getAb() {
      return ab;
    }

    public void setAb(String ab) {
      this.ab = ab;
    }

    public Date getIt() {
      return it;
    }

    public void setIt(Date it) {
      this.it = it;
    }

    public String getKv() {
      return kv;
    }

    public void setKv(String kv) {
      this.kv = kv;
    }

    public int getS() {
      return s;
    }

    public void setS(int s) {
      this.s = s;
    }

    public String getT() {
      return t;
    }

    public void setT(String t) {
      this.t = t;
    }

    public List<String> getTags() {
      return tags;
    }

    public void setTags(List<String> tags) {
      this.tags = tags;
    }

    public Date getUt() {
      return ut;
    }

    public void setUt(Date ut) {
      this.ut = ut;
    }

    public void setWdeRefs(List<WdeRef> wdeRefs) {
      this.wdeRefs = wdeRefs;
    }
    /**
     * Test if two Nodes represent the same node, considering ONLY name,type and id fields.<br>
     * TODO: how to deal with (id,null,null) and (null,name,type)...
     * 
     * @param node
     * @return
     */
    public boolean representSameEvent(Event event) {
      if (event == null) return false;
      if(id==event.getId())
        return true;
      else return false;
     
    }
    @Override
    public boolean equals(Object event){
      return this.id==((Event)event).getId();
    }
    @Override
    public int hashCode(){return this.id;}
}
