package ict.ada.gdb.rest.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.bind.Binder;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.PathGraph;
import ict.ada.common.util.ByteArray;
import ict.ada.common.util.Pair;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.PathQuerySpec;
import ict.ada.gdb.service.AdaGdbService;
import ict.ada.gdb.util.NodeIdConveter;

public class DfsPathGraph {
  private int heapsize;
  private PathMinHeap minheap;
  // 在 pathgraph中的node的id是不重复的，
  private PathGraph pathgraph;
  private Path path;
  private Map<Node, Iterator<Edge>> instack;
  private Node start;
  private Node end;
  private int degree;
  private int count;
  private Map<Node, Integer> heapNode;
  // private Map<Node, List<Path>> shortpath;
  private int lowdegree;

  // private
  // private
  // private
  // /
  //
  public DfsPathGraph(int heapsize, int degree, PathGraph pathgraph) {
    this.heapsize = heapsize;
    this.minheap = new PathMinHeap(heapsize);
    this.pathgraph = pathgraph;
    this.start = pathgraph.getGraphStart();
    this.end = pathgraph.getGraphEnd();
    path = new Path(start, getNodeWeight(start));
    instack = new HashMap<Node, Iterator<Edge>>();
    instack.put(start, getNodeAdjEdgeIter(start));
    this.degree = degree;
    count = 0;
    genMHeapNode();
    // lowdegree = 3;
    // this.internaldfs(lowdegree);
    // genShortPath();
  }

  private void genMHeapNode() {
    NodeMinHeap n = new NodeMinHeap(1000);
    heapNode = new HashMap<Node, Integer>();
    for (Entry<Node, Set<Node>> e : pathgraph.getAdjNodeList().entrySet()) {
      n.addNodeDegree(new NodeDegree(e.getKey(), e.getValue().size()));
    }
    for (NodeDegree nodedeg : n.getHeap()) {
      if (nodedeg == null) continue;
      heapNode.put(nodedeg.getNode(), nodedeg.getDegree());
    }
  }

  private void internaldfs(int limitdegree) {
    while (true) {
      if (path.getNodeList().size() == 0) break;
      Edge edge = null;
      // System.out.println(path.getNodeList().size());
      Iterator<Edge> iter = instack.get(getEndNodeInPath());
      if (!iter.hasNext()) {
        Node node = path.removeEndNode();
        instack.remove(node);
      } else {
        edge = iter.next();
        if (edge.getTail().equals(end)) {
          Path fullpath = new Path(path);
          fullpath.addNode(edge.getTail());
          calPathWeight(fullpath);
          fullpath.calWeight();
          // System.out.println(fullpath.getWeight());
          minheap.addPath(fullpath);
          count++;
          // System.out.println("add edge");
        } else if (!instack.containsKey(edge.getTail())) {
          if (this.path.getNodeList().size() < limitdegree) {
            path.addNode(edge.getTail(), getNodeWeight(edge.getTail()));
            instack.put(edge.getTail(), getNodeAdjEdgeIter(edge.getTail()));
          }
        }
      }
    }
  }

  public void maindfs() {
    while (true) {
      if (path.getNodeList().size() == 0) break;
      Edge edge = null;
      // System.out.println(path.getNodeList().size());
      Iterator<Edge> iter = instack.get(getEndNodeInPath());
      if (!iter.hasNext()) {
        Node node = path.removeEndNode();
        instack.remove(node);
      } else {
        edge = iter.next();
        if (edge.getTail().equals(end)) {
          Path fullpath = new Path(path);
          fullpath.addNode(edge.getTail());
          calPathWeight(fullpath);
          fullpath.calWeight();
          // System.out.println(fullpath.getWeight());
          minheap.addPath(fullpath);
          count++;
          // System.out.println("add edge");
        } else if (!instack.containsKey(edge.getTail())) {
          if (this.path.getNodeList().size() < degree - 1 && heapNode.containsKey(edge.getTail())) {
            path.addNode(edge.getTail(), getNodeWeight(edge.getTail()));
            instack.put(edge.getTail(), getNodeAdjEdgeIter(edge.getTail()));
          }
        }
      }
    }
  }

  /*
   * private void genShortPath() { //internaldfs(3); shortpath = new HashMap<Node, List<Path>>();
   * for (Path path : this.getMinheap().getHeap()) { if (path == null) continue; Path newPath=new
   * Path(path); newPath.removeFirst(); Node node = newPath.getNodeList().get(0); if
   * (shortpath.containsKey(node)) shortpath.get(node).add(newPath); else { List<Path> pathlist =
   * new ArrayList<Path>(); pathlist.add(newPath); shortpath.put(node, pathlist); }
   * 
   * } }
   */
  private void calPathWeight(Path path) {
    for (Node node : path.getNodeList())
      path.addTotalWeight(getNodeWeight(node));
  }

  private Node getEndNodeInPath() {
    int index = path.getNodeList().size() - 1;
    if (index < 0) System.out.println("****************************");
    return path.getNodeList().get(index);
  }

  public int getNodeWeight(Node node) {
    return pathgraph.getAdjNodeList().get(node).size();
  }

  public Iterator<Edge> getNodeAdjEdgeIter(Node node) {
    return pathgraph.getAdjacentEdgeList().get(node).iterator();
  }

  /**
   * @return the heapsize
   */
  public int getHeapsize() {
    return heapsize;
  }

  /**
   * @param heapsize
   *          the heapsize to set
   */
  public void setHeapsize(int heapsize) {
    this.heapsize = heapsize;
  }

  /**
   * @return the minheap
   */
  public PathMinHeap getMinheap() {
    return minheap;
  }

  /**
   * @param minheap
   *          the minheap to set
   */
  public void setMinheap(PathMinHeap minheap) {
    this.minheap = minheap;
  }

  public static void main(String[] args) throws GdbException, FileNotFoundException {
    byte[] startid = NodeIdConveter.checkAndtoBytes("c801c42a30607f3dc25f03af7d27f178b0b1");
    byte[] endid = NodeIdConveter.checkAndtoBytes("c8018c4e85d50e11f02dea25c77d9f66e71b");
    PrintStream ps = new PrintStream(new File("/home/wangqianbo/test/test"));
    AdaGdbService adaGdbService = new AdaGdbService();
    PathQuerySpec.PathQuerySpecBuilder builder = new PathQuerySpec.PathQuerySpecBuilder(startid,
        endid);
    builder.maxPathLength(3);
    long start = System.currentTimeMillis();
    PathGraph pathgraph = adaGdbService.queryPathGraph(builder.build());
    long end = System.currentTimeMillis();
    System.out.println(end - start);
    long start1 = System.currentTimeMillis();
    DfsPathGraph d = new DfsPathGraph(10, 3, pathgraph);
    d.maindfs();
    long end1 = System.currentTimeMillis();
    System.out.println(end1 - start1);
    for (Path path : d.getMinheap().getHeap()) {
      if (path == null) {
        System.out.println("weird");
        continue;
      }
      for (Node node : path.getNodeList()) {
        ps.print(NodeIdConveter.toString(node.getId()));
        ps.print("-->");
      }
      ps.println("============================");
    }
  }
}
