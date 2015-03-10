package ict.ada.gdb.rest.beans.model;

import ict.ada.gdb.util.NodeIdConveter;

import java.util.List;

public class Place {

  private String parentId;
  private String id;
  private String name;
  private List<String> sname;
  private String type = "location";
  private String longitude;
  private String latitude;

  public String getParentId() {
    return parentId;
  }

  public Place(ict.ada.common.model.Node node, String longitude, String latitude, String parentId) {
    this.parentId = parentId;
    this.id = NodeIdConveter.toString(node.getId());
    this.latitude = latitude;
    this.longitude = longitude;
    this.name = node.getName();
    this.sname = node.getSnames();
  }

  public void setParentId(String parentId) {
    this.parentId = parentId;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getSname() {
    return sname;
  }

  public void setSname(List<String> sname) {
    this.sname = sname;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getLongitude() {
    return longitude;
  }

  public void setLongitude(String longitude) {
    this.longitude = longitude;
  }

  public String getLatitude() {
    return latitude;
  }

  public void setLatitude(String latitude) {
    this.latitude = latitude;
  }

}
