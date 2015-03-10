package ict.ada.gdb.rest.beans;

import ict.ada.common.model.Relation;
import ict.ada.common.model.RelationGraph;
import ict.ada.gdb.rest.beans.model.EdgeInfo;
import ict.ada.gdb.rest.beans.model.Node;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.rest.util.NodeIdConveter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class GetRelationTimeLineByNodeIdBean {
  private String errorCode = "success";
  private List<Node> timenodes = null;

  public GetRelationTimeLineByNodeIdBean(RelationGraph relationGraph) {
    timenodes = new ArrayList<Node>();
    int count = 0;
    Iterator<ict.ada.common.model.Edge> edgeIter = relationGraph.getCenterEdges().iterator();
    for (ict.ada.common.model.Node node : relationGraph.getOuterNodes()) {
      Node node1 = new Node(node, null);
      node1.addEdgeInfo(edgeIter.next());
      timenodes.add(node1);
    }
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
   * @return the timenodes
   */
  public List<Node> getTimenodes() {
    return timenodes;
  }

  /**
   * @param timenodes
   *          the timenodes to set
   */
  public void setTimenodes(List<Node> timenodes) {
    this.timenodes = timenodes;
  }

}
