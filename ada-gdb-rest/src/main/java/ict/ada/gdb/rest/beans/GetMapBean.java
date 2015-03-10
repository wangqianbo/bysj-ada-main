package ict.ada.gdb.rest.beans;

import ict.ada.gdb.rest.beans.model.Edge;
import ict.ada.gdb.rest.beans.model.Place;

import java.util.ArrayList;
import java.util.List;

public class GetMapBean {
  private List<Place> locationList;
  private List<Edge> edgeList;
  private String errorCode = "success";

  public GetMapBean() {
    locationList = new ArrayList<Place>();
    edgeList = new ArrayList<Edge>();
  }

  public void addEdge(ict.ada.common.model.Edge edge) {
    edgeList.add(new Edge(edge));
  }

  public void addPlace(ict.ada.common.model.Node node, String longitude, String latitude,
      String parentId) {
    locationList.add(new Place(node, longitude, latitude, parentId));
  }

  public List<Place> getLocationList() {
    return locationList;
  }

  public void setLocationList(List<Place> locationList) {
    this.locationList = locationList;
  }

  public List<Edge> getEdgeList() {
    return edgeList;
  }

  public void setEdgeList(List<Edge> edgeList) {
    this.edgeList = edgeList;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

}
