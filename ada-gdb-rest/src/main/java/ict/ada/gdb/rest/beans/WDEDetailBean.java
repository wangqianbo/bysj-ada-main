package ict.ada.gdb.rest.beans;

import java.util.ArrayList;
import java.util.List;

public class WDEDetailBean {

  private String errorCode;
  private Detail detail;

  public WDEDetailBean() {
    errorCode = "success";
  }

  public WDEDetailBean(ScholarWDEDetailBean scholarBean) {
    this.errorCode = "success";
    this.detail = new Detail();
    detail.setAbstract(scholarBean.getAbstract());
    detail.setAuthor(scholarBean.getAuthor().toString());
    detail.setContent(scholarBean.isContent());
    if (scholarBean.getFetch_time() != null && !scholarBean.getFetch_time().equals("")) detail
        .setFetch_time(Long.parseLong(scholarBean.getFetch_time()));
    else detail.setFetch_time(0);
    detail.setHtml(scholarBean.isHtml());
    detail.setTitle(scholarBean.getTitle());
    detail.setUrl(scholarBean.getUrl());
  }

  public WDEDetailBean(WDEDetailBeanError bean) {
    this.errorCode = bean.getErrorCode();
    this.detail = new Detail(bean.getDetail());
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
    private long pub_time;
    private boolean content = false;
    private boolean html = false;

    public Detail(WDEDetailBeanError.Detail detail) {
      this.url = detail.getUrl();
      this.Abstract = detail.getAbstract();
      this.title = detail.getTitle();
      this.author = detail.getAuthor();
      this.fetch_time = detail.getFetch_time();
      this.content = detail.isContent();
      this.html = detail.isHtml();
      this.pub_time = detail.getPub_time();
    }

    public Detail() {
    };

    public String getUrl() {
      return url;
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

    public String getAuthor() {
      return author;
    }

    public void setAuthor(String author) {
      this.author = author;
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
      return content;
    }

    /**
     * @param content
     *          the content to set
     */
    public void setContent(boolean content) {
      this.content = content;
    }

    /**
     * @return the html
     */
    public boolean isHtml() {
      return html;
    }

    /**
     * @param html
     *          the html to set
     */
    public void setHtml(boolean html) {
      this.html = html;
    }
  }

}
