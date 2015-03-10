package ict.ada.gdb.rest.beans;

import java.util.ArrayList;
import java.util.List;

public class ScholarWDEDetailBean {
  private String url; // wde url
  private String Abstract;
  private String title;
  private List<String> author;
  private String fetch_time = null;
  private boolean content = false;
  private boolean html = false;

  public ScholarWDEDetailBean() {
    author = new ArrayList<String>();
    fetch_time = null;
    content = false;
    html = false;
    url = null;
  }

  public void initAuthorList() {
    author = new ArrayList<String>();
  }

  public void addAuthor(String author) {
    this.author.add(author);
  }

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

  /**
   * @return the author
   */
  public List<String> getAuthor() {
    return author;
  }

  /**
   * @param author
   *          the author to set
   */
  public void setAuthor(List<String> author) {
    this.author = author;
  }

  /**
   * @return the fetch_time
   */
  public String getFetch_time() {
    return fetch_time;
  }

  /**
   * @param fetch_time
   *          the fetch_time to set
   */
  public void setFetch_time(String fetch_time) {
    this.fetch_time = fetch_time;
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
