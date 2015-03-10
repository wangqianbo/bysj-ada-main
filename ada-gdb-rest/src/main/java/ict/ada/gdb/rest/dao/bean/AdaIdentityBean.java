package ict.ada.gdb.rest.dao.bean;

import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

public class AdaIdentityBean {
  @JsonProperty("_id")
  private String id;
  private String n;// name
  private String sn;// sname
  private String addl;// additional
  private List<String> tags;// 用于检索
  private List<String> ents;// entitys
  private List<String> docs;// 相关文档WDEID
  @JsonIgnore
  public final static NullAdaIdentityBean NULLBEAN = new NullAdaIdentityBean();

  public AdaIdentityBean() {
    this.setId("None");
    this.setN("None");
    this.setSn("None");
    this.setAddl("None");
    this.setTags(Collections.<String> emptyList());
    this.setDocs(Collections.<String> emptyList());
    this.setEnts(Collections.<String> emptyList());

  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getN() {
    return n;
  }

  public void setN(String n) {
    this.n = n;
  }

  public String getSn() {
    return sn;
  }

  public void setSn(String sn) {
    this.sn = sn;
  }

  public String getAddl() {
    return addl;
  }

  public void setAddl(String addl) {
    this.addl = addl;
  }

  public List<String> getTags() {
    return tags;
  }

  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  public List<String> getEnts() {
    return ents;
  }

  public void setEnts(List<String> ents) {
    this.ents = ents;
  }

  public List<String> getDocs() {
    return docs;
  }

  public void setDocs(List<String> docs) {
    this.docs = docs;
  }

  public static class NullAdaIdentityBean extends AdaIdentityBean {
    public NullAdaIdentityBean() {
      this.setId("None");
      this.setN("None");
      this.setSn("None");
      this.setAddl("None");
      this.setTags(Collections.<String> emptyList());
      this.setDocs(Collections.<String> emptyList());
      this.setEnts(Collections.<String> emptyList());
    }
  }
}
