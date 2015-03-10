package ict.ada.gdb.rest.beans;

public class GetHtml_urlBean {
  private String errorCode = "success";
  private String html;

  public GetHtml_urlBean(String html) {
    this.html = html;
  }

  /**
   * @return the errorCode
   */
  public String getErrorCode() {
    return errorCode;
  }

  /**
   * @param errorCode
   *          the errorCode to set
   */
  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  /**
   * @return the html_url
   */
  public String getHtml() {
    return html;
  }

  /**
   * @param html_url
   *          the html_url to set
   */
  public void setHtml(String html) {
    this.html = html;
  }

}
