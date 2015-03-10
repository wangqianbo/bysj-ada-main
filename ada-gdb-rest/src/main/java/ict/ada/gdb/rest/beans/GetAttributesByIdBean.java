/**
 * 
 */
package ict.ada.gdb.rest.beans;

//import ict.ada.common.model.NodeAttribute;

import ict.ada.gdb.rest.beans.model.NodeAttribute;

import java.util.ArrayList;
import java.util.List;

/**
 * @author forhappy
 * 
 */
public class GetAttributesByIdBean {

  private String errorCode = "success";
  private List<NodeAttribute> attrsList;

  /**
   * @return the errorCode
   */
  public GetAttributesByIdBean() {
    super();
    attrsList = new ArrayList<NodeAttribute>();
  }

  public GetAttributesByIdBean(String errorCode, List<NodeAttribute> attrsList) {
    this.errorCode = errorCode;
    this.attrsList = attrsList;
  }

  public void initAttrsList() {
    attrsList = new ArrayList<NodeAttribute>();
  }

  public void addAttrsList(NodeAttribute attr) {
    attrsList.add(attr);
  }

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
   * @param attrsList
   *          the attrsList to set
   */
  public void setAttrsList(List<NodeAttribute> attrsList) {
    this.attrsList = attrsList;
  }

  /**
   * 
   */
  /**
   * @return the attrsList
   */
  public List<NodeAttribute> getAttrsList() {
    return attrsList;
  }

}
