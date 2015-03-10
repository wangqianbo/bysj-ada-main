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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hdfs.util.ByteArray;
import org.javatuples.Pair;
import org.javatuples.Triplet;

;

public class ReleventEvent {
  private List<Triplet<Node, List<Node>, Integer>> similarEvents; // 最后的结果
  private Node startEvent; // 开始事件,种子事件
  private Set<Attribute> nodeTypes; // elements的types
  private List<Node> elements; // 取出的elements
  private AdaGdbService adaGdbService; //
  private int len;

  public ReleventEvent(Node startEvent, Set<Attribute> nodeTypes, AdaGdbService adaGdbService,
      int len) {
    this.startEvent = startEvent;
    this.nodeTypes = nodeTypes;
    this.adaGdbService = adaGdbService;
    this.len = len;
  }

  /**
   * 计算相似事件.
   * 
   * @throws GdbException
   */
  public void genSimilarEvents() throws GdbException {
    // 1,首先根据nodeTypes获取elements
    this.elements = genElements(startEvent);
    List<RelQuerySpec> specList = new ArrayList<RelQuerySpec>(elements.size());
    for (Node node : elements) {
      RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(node);
      relbuilder.resultSize(10000).attribute(Attribute.EVENT);
      specList.add(relbuilder.build());
    }
    System.out.println("specList.size:" + specList.size());
    // 2,依据elements批量获取相关的事件
    int batchSize = specList.size() / 16 + 1;
    List<RelationGraph> graphs = adaGdbService.queryRelationGraphsInParallel(specList, batchSize);
    HashMap<ByteArray, List<Node>> similarEventToElementMap = new HashMap<ByteArray, List<Node>>();
    for (RelationGraph graph : graphs) {
      for (Node node : graph.getOuterNodes()) {
        ByteArray key = new ByteArray(node.getId());// eventId
        List<Node> value = similarEventToElementMap.get(key);
        if (value != null) value.add(graph.getCenterNode());
        else {
          value = new ArrayList<Node>();
          value.add(graph.getCenterNode());
          similarEventToElementMap.put(key, value);
        }
      }
    }
    List<Pair<Node, List<Node>>> similarEventsInter = new ArrayList<Pair<Node, List<Node>>>(
        similarEventToElementMap.size());
    ByteArray startEventId = new ByteArray(startEvent.getId());
    for (Entry<ByteArray, List<Node>> entry : similarEventToElementMap.entrySet())
      if (!entry.getKey().equals(startEventId)) similarEventsInter.add(new Pair<Node, List<Node>>(
          new Node(entry.getKey().getBytes()), entry.getValue()));
    // 3.依据elements的重合度排序!
    Collections.sort(similarEventsInter, new Comparator<Pair<Node, List<Node>>>() {
      @Override
      public int compare(Pair<Node, List<Node>> similarEvent1, Pair<Node, List<Node>> similarEvent2) {
        return similarEvent2.getValue1().size() - similarEvent1.getValue1().size();
      }
    });
    System.out.println("similarEventsInter.size : " + similarEventsInter.size());
    // 4,计算相似度,取1.5*len的点计算.
    int callen = (int) (len * 1.5);
    callen = callen <= similarEventsInter.size() ? callen : similarEventsInter.size();
    similarEventsInter = similarEventsInter.subList(0, callen);

    System.out.println("11similarEventsInter.size  : " + similarEventsInter.size());
    List<Node> similarEventsNodes = new ArrayList<Node>(similarEventsInter.size());
    // 4.1 获取要计算点的elements.
    for (Pair<Node, List<Node>> similarEvent : similarEventsInter)
      similarEventsNodes.add(similarEvent.getValue0());
    List<List<Node>> nodesElements = genMutiElements(similarEventsNodes);
    System.out.println("nodesElements.size : " + nodesElements.size());
    similarEvents = new ArrayList<Triplet<Node, List<Node>, Integer>>(
        similarEventToElementMap.size());
    Iterator<List<Node>> nodesElementsIter = nodesElements.iterator();
    // 4.2 相似度计算
    for (Pair<Node, List<Node>> similarEvent : similarEventsInter) {
      List<Node> similarEventElements = nodesElementsIter.next();
      similarEvents.add(new Triplet<Node, List<Node>, Integer>(similarEvent.getValue0(),
          similarEvent.getValue1(), calSimilarEvent(this.elements.size(),
              similarEventElements.size(), similarEvent.getValue1().size())));
    }

    System.out.println("similarEvents.size : " + similarEvents.size());

    // 4.3 根据相似度重新排序
    Collections.sort(similarEvents, new Comparator<Triplet<Node, List<Node>, Integer>>() {
      @Override
      public int compare(Triplet<Node, List<Node>, Integer> similarEvent1,
          Triplet<Node, List<Node>, Integer> similarEvent2) {
        return similarEvent2.getValue2() - similarEvent1.getValue2();
      }
    });
    // 4.4 获取最终结果
    len = len <= similarEvents.size() ? len : similarEvents.size();
    similarEvents = similarEvents.subList(0, len);
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
    RelationGraph graph = adaGdbService.queryRelationGraph(relbuilder.build());
    List<Node> elements = new ArrayList<Node>(graph.getOuterNodes().size());
    System.out.println(graph.getOuterNodes().size());
    for (Node node : graph.getOuterNodes()) {
      if (nodeTypes.contains(node.getType().getAttribute())) elements.add(node);
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

  public Node getStartEvent() {
    return startEvent;
  }

  public Set<Attribute> getNodeTypes() {
    return nodeTypes;
  }

  public List<Node> getElements() {
    return elements;
  }

  public List<Triplet<Node, List<Node>, Integer>> getSimilarEvents() {
    return similarEvents;
  }

}
