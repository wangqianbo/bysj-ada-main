package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.WDEDetailBean.Detail;

import java.util.ArrayList;
import java.util.List;

public class WDEDetailBeanError {

  private String errorCode;
  private Detail detail;

  public WDEDetailBeanError() {
    detail = new Detail();
    errorCode = "success";
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public Detail getDetail() {
    return detail;
  }

  public void setDetail(Detail detail) {
    this.detail = detail;
  }

  public static class Detail {
    private String url; // wde url
    private String Abstract;
    private String title;
    private String author;
    private long fetch_time;
    private String content;
    private String html;
    private long pub_time;

    public void setAuthor(String author) {
      this.author = author;
    }

    public String getUrl() {
      return url;
    }

    public String getAuthor() {
      return author;
    }

    /**
     * @param url
     *          the url to set
     */
    public void setUrl(String url) {
      this.url = url;
    }

    /**
     * @return the abstract
     */
    public String getAbstract() {
      return Abstract;
    }

    /**
     * @param abstract1
     *          the abstract to set
     */
    public void setAbstract(String abstract1) {
      Abstract = abstract1;
    }

    /**
     * @return the title
     */
    public String getTitle() {
      return title;
    }

    /**
     * @param title
     *          the title to set
     */
    public void setTitle(String title) {
      this.title = title;
    }

    public long getFetch_time() {
      return fetch_time;
    }

    public void setFetch_time(long fetch_time) {
      this.fetch_time = fetch_time;
    }

    public long getPub_time() {
      return pub_time;
    }

    public void setPub_time(long pub_time) {
      this.pub_time = pub_time;
    }

    /**
     * @return the content
     */
    public boolean isContent() {
      if (this.content != null && this.content.equals("true")) return true;
      else return false;
    }

    /**
     * @param content
     *          the content to set
     */
    public void setContent(String content) {
      this.content = content;
    }

    /**
     * @return the html
     */
    public boolean isHtml() {
      if (this.html != null && this.html.equals("true")) return true;
      else return false;
    }

    /**
     * @param html
     *          the html to set
     */
    public void setHtml(String html) {
      this.html = html;
    }
  }
}
