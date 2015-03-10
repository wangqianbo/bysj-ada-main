package ict.ada.gdb.rest.beans;

import ict.ada.common.model.Relation;
import ict.ada.common.model.RelationType;
import ict.ada.gdb.rest.beans.model.EdgeInfo;
import ict.ada.gdb.util.NodeIdConveter;

import java.util.ArrayList;
import java.util.List;

public class GetEdgeInfomationByIdBean {
  private String errorCode;
  private List<EdgeInfo> infos;

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
   * @return the infos
   */
  public List<EdgeInfo> getInfos() {
    return infos;
  }

  public GetEdgeInfomationByIdBean() {
    errorCode = "success";
    infos = new ArrayList<EdgeInfo>();
  }

  public void addInfos(EdgeInfo edgeInfo) {
    infos.add(edgeInfo);
  }

  /**
   * @param infos
   *          the infos to set
   */
  public void setInfos(List<EdgeInfo> infos) {
    this.infos = infos;
  }

}
