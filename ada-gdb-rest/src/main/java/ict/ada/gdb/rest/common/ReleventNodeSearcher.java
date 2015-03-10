package ict.ada.gdb.rest.common;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.RelationGraph;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.service.AdaGdbService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hdfs.util.ByteArray;
import org.javatuples.Pair;
import org.javatuples.Triplet;

;

public class ReleventNodeSearcher {
  private List<Triplet<Node, List<Node>, Integer>> similarNodes; // 最后的结果
  private Node startNode; // 开始节点,种子事件
  private Set<Attribute> nodeTypes; // elements的types
  private List<Node> elements; // 取出的elements
  private  HashMap<ByteArray,Node> elementsMap;
  private AdaGdbService adaGdbService; //
  private int len;
  private int elementSize;
  
  
  public ReleventNodeSearcher(Node startNode, Set<Attribute> nodeTypes, AdaGdbService adaGdbService,
      int len,int elementSize) {
    this.startNode = startNode;
    this.nodeTypes = nodeTypes;
    this.adaGdbService = adaGdbService;
    this.len = len;
    this.elementSize = elementSize;
    this.elementsMap = new HashMap<ByteArray,Node>();
  }

  /**
   * 计算相似事件.
   * 
   * @throws GdbException
   */
  public void genSimilarEvents() throws GdbException {
    // 1,首先根据nodeTypes获取elements
    this.elements = genElements(startNode);
    List<RelQuerySpec> specList = new ArrayList<RelQuerySpec>(elements.size());
    for (Node node : elements) {
      RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(node);
      relbuilder.resultSize(10000).attribute(startNode.getType().getAttribute());
      specList.add(relbuilder.build());
    }
    System.out.println("specList.size:" + specList.size());
    // 2,依据elements批量获取相关的事件
    int batchSize = specList.size() / 16 + 1;
    List<RelationGraph> graphs = adaGdbService.queryRelationGraphsInParallel(specList, batchSize);
    HashMap<ByteArray, List<Node>> similarNodeToElementMap = new HashMap<ByteArray, List<Node>>();
    for (RelationGraph graph : graphs) {
      for (Node node : graph.getOuterNodes()) {
        ByteArray key = new ByteArray(node.getId());// eventId
        List<Node> value = similarNodeToElementMap.get(key);
        if (value != null) value.add(graph.getCenterNode());
        else {
          value = new ArrayList<Node>();
          value.add(graph.getCenterNode());
          similarNodeToElementMap.put(key, value);
        }
      }
    }
    List<Pair<Node, List<Node>>> similarNodesInter = new ArrayList<Pair<Node, List<Node>>>(
        similarNodeToElementMap.size());
    ByteArray startNodeId = new ByteArray(startNode.getId());
    for (Entry<ByteArray, List<Node>> entry : similarNodeToElementMap.entrySet())
      if (!entry.getKey().equals(startNodeId)) similarNodesInter.add(new Pair<Node, List<Node>>(
          new Node(entry.getKey().getBytes()), entry.getValue()));
    // 3.依据elements的重合度排序!
    Collections.sort(similarNodesInter, new Comparator<Pair<Node, List<Node>>>() {
      @Override
      public int compare(Pair<Node, List<Node>> similarNode1, Pair<Node, List<Node>> similarEvent2) {
        return similarEvent2.getValue1().size() - similarNode1.getValue1().size();
      }
    });
    System.out.println("similarEventsInter.size : " + similarNodesInter.size());
    // 4,计算相似度,取1.5*len的点计算.
    int callen = (int) (len * 1.5);
    callen = callen <= similarNodesInter.size() ? callen : similarNodesInter.size();
    similarNodesInter = similarNodesInter.subList(0, callen);

    System.out.println("11similarEventsInter.size  : " + similarNodesInter.size());
    List<Node> similarEventsNodes = new ArrayList<Node>(similarNodesInter.size());
    // 4.1 获取要计算点的elements.
    for (Pair<Node, List<Node>> similarNode : similarNodesInter)
      similarEventsNodes.add(similarNode.getValue0());
    List<List<Node>> nodesElements = genMutiElements(similarEventsNodes);
    List<List<Node>>  overlapElements = getOverlapNodesList(nodesElements);
    //TODO 计算重合的element。
    System.out.println("nodesElements.size : " + nodesElements.size());
    similarNodes = new ArrayList<Triplet<Node, List<Node>, Integer>>(
        similarNodeToElementMap.size());
    Iterator<List<Node>> nodesElementsIter = nodesElements.iterator();
    // 4.2 相似度计算
    Iterator<List<Node>> overlapCountIter = overlapElements.iterator(); 
    for (Pair<Node, List<Node>> similarNode : similarNodesInter) {
      List<Node> similarEventElements = nodesElementsIter.next();
      List<Node> overlap =  overlapCountIter.next();
      similarNodes.add(new Triplet<Node, List<Node>, Integer>(similarNode.getValue0(),
          overlap, calSimilarEvent(this.elementsMap.size(),
              similarEventElements.size(), overlap.size())));
    }

    System.out.println("similarNodes.size : " + similarNodes.size());

    // 4.3 根据相似度重新排序
    Collections.sort(similarNodes, new Comparator<Triplet<Node, List<Node>, Integer>>() {
      @Override
      public int compare(Triplet<Node, List<Node>, Integer> similarNode1,
          Triplet<Node, List<Node>, Integer> similarNode2) {
        return similarNode2.getValue2() - similarNode1.getValue2();
      }
    });
    // 4.4 获取最终结果
    len = len <= similarNodes.size() ? len : similarNodes.size();
    similarNodes = similarNodes.subList(0, len);
  }

  /**
   * 依据nodeType 获取elements
   * 
   * @return
   * @throws GdbException
   */
  private List<Node> genElements(Node seedNode) throws GdbException {
    RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(seedNode);
    relbuilder.resultSize(10000);
    if (nodeTypes.size() == 1) relbuilder.attribute((Attribute) nodeTypes.toArray()[0]);
    else relbuilder.attribute(Attribute.ANY);
    relbuilder.useRelRank(true);
    RelationGraph graph = adaGdbService.queryRelationGraph(relbuilder.build());
    List<Node> elements = new ArrayList<Node>(this.elementSize);
    System.out.println(graph.getOuterNodes().size());
    int count = 0;
    for (Node node : graph.getOuterNodes()) {
      if (nodeTypes.contains(node.getType().getAttribute())) {
        if(count < this.elementSize)
               elements.add(node);
       this.elementsMap .put(new ByteArray(node.getId()),node);
      }
    }
    return elements;
  }

  
  private List<List<Node>> genMutiElements(List<Node> nodes) throws GdbException {
    List<RelQuerySpec> specList = new ArrayList<RelQuerySpec>(nodes.size());
    for (Node node : nodes) {
      RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(node);
      relbuilder.resultSize(10000);
      if (nodeTypes.size() == 1) relbuilder.attribute((Attribute) nodeTypes.toArray()[0]);
      else relbuilder.attribute(Attribute.ANY);
      specList.add(relbuilder.build());
    }
    int batchSize = specList.size() / 16 + 1;
    List<RelationGraph> graphs = adaGdbService.queryRelationGraphsInParallel(specList, batchSize);
    List<List<Node>> nodesElements = new ArrayList<List<Node>>(nodes.size());
    for (RelationGraph graph : graphs) {
      List<Node> elements = new ArrayList<Node>(graph.getOuterNodes().size());
      System.out.println(graph.getOuterNodes().size());
      for (Node node : graph.getOuterNodes()) {
        if (nodeTypes.contains(node.getType().getAttribute())) elements.add(node);
      }
      nodesElements.add(elements);
    }
    return nodesElements;

  }

  private List<List<Node>> getOverlapNodesList(List<List<Node>> elementsList){
    List<List<Node>> overlapNodesList = new ArrayList<List<Node>>(elementsList.size());
    for(List<Node> elements : elementsList){
     List<Node> overlapNodes =  new ArrayList<Node>();
      for(Node element : elements){
        ByteArray id = new ByteArray(element.getId());
        Node node = elementsMap.get(id);
        if(node!=null)
          overlapNodes.add(node);
      }
      overlapNodesList.add(overlapNodes);
    }
    return overlapNodesList;
  }
  
  /**
   * 计算相似度
   * */
  private int calSimilarEvent(int elements1, int elements2, int similarn) {
    System.out.println("elements1 = " + elements1 + "elements2= " + elements2 + "similarn= "
        + similarn);
    double similarity = similarn / (Math.pow(elements1, 0.5) * Math.pow(elements2, 0.5));
    int percent = (int) (similarity * 100);
    return percent;
  }

  public List<Triplet<Node, List<Node>, Integer>> getSimilarNodes() {
    return similarNodes;
  }

  public Node getStartNode() {
    return startNode;
  }

  public Set<Attribute> getNodeTypes() {
    return nodeTypes;
  }

  public List<Node> getElements() {
    return elements;
  }

  

  public HashMap<ByteArray, Node> getElementsMap() {
    return elementsMap;
  }


  public int getLen() {
    return len;
  }

  public int getElementSize() {
    return elementSize;
  }

  
}
