package ict.ada.gdb.dao;

import ict.ada.common.model.Node;
import ict.ada.common.model.PathGraph;
import ict.ada.common.util.ByteArray;
import ict.ada.common.util.MapTool;
import ict.ada.common.util.Timer;
import ict.ada.gdb.common.PathQuerySpec;
import ict.ada.gdb.common.PathQuerySpec.PathQuerySpecBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.common.base.Preconditions;

/**
 * Encapsulate the algorithm for path search.
 * This will search for a sub-graph instead of a list of paths.
 * 
 */
public class GdbPathSearcher {
  private static Log LOG = LogFactory.getLog(GdbPathSearcher.class);

  private ExecutorService exec;
  private PathQuerySpec spec;
  private HBaseEdgeDAO dao;

  public GdbPathSearcher(PathQuerySpec spec, HBaseEdgeDAO dao, ExecutorService exec) {
    if (spec == null) throw new NullPointerException("null spec");
    if (dao == null) throw new NullPointerException("null dao");
    if (exec == null) throw new NullPointerException("null exec");
    this.spec = spec;
    this.dao = dao;
    this.exec = exec;
  }

  /**
   * Store distance info for a node
   */
  private static class NodeInfo {
    public static final int LEFT_SIDE_IDX = 0;
    public static final int RIGHT_SIDE_IDX = 1;
    /**
     * dis[0]= min distance to left start node
     * dis[1]= min distance to right end node
     */
    private int dis[] = new int[] { -1, -1 };

    public NodeInfo setLeftDis(int d) {
      if (dis[0] >= 0) throw new IllegalStateException("dis[0]=" + dis[0]);
      dis[0] = d;
      return this;
    }

    public NodeInfo setRightDis(int d) {
      if (dis[1] >= 0) throw new IllegalStateException("dis[1]=" + dis[1]);
      dis[1] = d;
      return this;
    }

    public NodeInfo set(int idx, int value) {
      if (dis[idx] >= 0) throw new IllegalStateException("dis[" + idx + "]=" + dis[idx]);
      dis[idx] = value;
      return this;
    }

    public int getLeftDis() {
      return dis[0];
    }

    public int getRightDis() {
      return dis[1];
    }

    public boolean candidateNode(int limit) {
      return dis[0] >= 0 && dis[1] >= 0 && dis[0] + dis[1] <= limit;
    }

    public boolean pathLenEqualTo(int limit) {
      return dis[0] >= 0 && dis[1] >= 0 && dis[0] + dis[1] == limit;
    }

    @Override
    public String toString() {
      return "dis[0]=" + dis[0] + " dis[1]=" + dis[1];
    }
  }

  private Map<ByteArray, NodeInfo> nodesInfo = new HashMap<ByteArray, GdbPathSearcher.NodeInfo>();

  /** Cache each Node's related Nodes that we get from HBase */
  private ConcurrentHashMap<ByteArray, List<ByteArray>> relatedNodesCache = new ConcurrentHashMap<ByteArray, List<ByteArray>>(
      30000, 0.75f, MAX_THREAD + 2);

  private static final int MAX_THREAD = 230;

  private AtomicLong totalHBaseReq = new AtomicLong();
  private AtomicLong totalHBaseReqTime = new AtomicLong();

  private List<ByteArray> expandNode(ByteArray nodeId) {
    List<ByteArray> cur = relatedNodesCache.get(nodeId);
    if (cur != null) {
      return cur;
    } else {
      try {
        long start = Timer.now();
        List<byte[]> x = dao.getRelatedNodeIdList(new Node(nodeId.getBytes()), spec);
        totalHBaseReqTime.addAndGet(Timer.nsSince(start));
        totalHBaseReq.incrementAndGet();
        // List<ByteArray> res = new ArrayList<ByteArray>(x.size());//poor performance
        List<ByteArray> res = new LinkedList<ByteArray>();
        for (byte[] bs : x) {
          res.add(new ByteArray(bs));
        }
        relatedNodesCache.putIfAbsent(nodeId, res);
        return res;
      } catch (Exception e) {
        throw new RuntimeException("Fail to get related node.", e);
      }
    }
  }

  public double getHBaseReqTimeInMs() {
    return totalHBaseReqTime.longValue() * 1.0 / 1000000;
  }

  private List<List<ByteArray>> concurrentExpand(ArrayList<ByteArray> nodeIdsToExpand) {
    long start = Timer.now();
    final int minTaskPerThread = 25;
    int t = nodeIdsToExpand.size() / MAX_THREAD;
    int taskPerThread = t < minTaskPerThread ? minTaskPerThread : t;

    List<Future<List<ByteArray>>> futureList = new ArrayList<Future<List<ByteArray>>>();
    for (int i = 0; i < nodeIdsToExpand.size(); i += taskPerThread) {
      final ArrayList<ByteArray> batch;
      if (i + taskPerThread >= nodeIdsToExpand.size()) {
        batch = new ArrayList<ByteArray>(nodeIdsToExpand.subList(i, nodeIdsToExpand.size()));
      } else {
        batch = new ArrayList<ByteArray>(nodeIdsToExpand.subList(i, i + taskPerThread));
      }
      futureList.add(exec.submit(new Callable<List<ByteArray>>() {
        @Override
        public List<ByteArray> call() throws Exception {
          List<ByteArray> result = new ArrayList<ByteArray>();
          for (ByteArray node : batch) {
            result.addAll(expandNode(node));
          }
          return result;
        }
      }));
    }
    List<List<ByteArray>> resultList = new ArrayList<List<ByteArray>>(futureList.size());
    for (Future<List<ByteArray>> future : futureList) {
      try {
        resultList.add(future.get());
      } catch (Exception e) {
        throw new RuntimeException("Fail to get related node.", e);
      }
    }
    LOG.info("Concurrent expanding for " + nodeIdsToExpand.size() + " Nodes finished in "
        + Timer.msSince(start) + "ms");
    return resultList;
  }

  public PathGraph search() {
    long start = Timer.now();
    PathGraph result = internalSearch(spec.getStartNode().getId(), spec.getEndNode().getId(),
        spec.getMaxPathLength());
    LOG.info(result.getGraphStatistics());
    LOG.info("Search For PathGraph return in " + Timer.msSince(start) + "ms. Spec=" + spec);
    return result;
  }

  /**
   * 
   * @param startId
   * @param endId
   * @param pathLenMax
   * @return node id set in the bfs graph
   */
  private PathGraph internalSearch(byte[] startId, byte[] endId, int pathLenMax) {
    LOG.debug("Starting BFS....  start=" + Hex.encodeHexString(startId) + " end="
        + Hex.encodeHexString(endId));

    // Our algorithm consists of multiple phases

    /*
     * Phase 1: two-way BFS with HBase data
     */
    Set<ByteArray> leftVisited = new HashSet<ByteArray>(30000);
    Set<ByteArray> rightVisited = new HashSet<ByteArray>(30000);

    ByteArray bfsStart = new ByteArray(startId);
    ByteArray bfdEnd = new ByteArray(endId);

    // visit the start and end node
    leftVisited.add(bfsStart);
    rightVisited.add(bfdEnd);
    nodesInfo.put(bfsStart, new NodeInfo().setLeftDis(0));
    nodesInfo.put(bfdEnd, new NodeInfo().setRightDis(0));

    int curLen = 0, leftLen = 0, rightLen = 0;
    long totalDegree = 0, totalNode = 0;
    long start = Timer.now();
    ArrayList<ByteArray> leftQueue = new ArrayList<ByteArray>();
    ArrayList<ByteArray> rightQueue = new ArrayList<ByteArray>();
    leftQueue.add(bfsStart);
    rightQueue.add(bfdEnd);
    while (curLen <= pathLenMax - 1) {
      ArrayList<ByteArray> curQueue;
      Set<ByteArray> curVisited;
      int curDistance;

      boolean useLeft = true;
      // Determine which side to expand. Always expand the side with less nodes in queue.
      // TODO: Nodes in queue may have already been expanded and are in relatedNodesCache, so queue
      // size does not necessarily represents cost.
      if (rightQueue.size() != 0 && leftQueue.size() != 0) {
        if (rightQueue.size() < leftQueue.size()
            || (rightQueue.size() == leftQueue.size() && rightVisited.size() < leftVisited.size())) {
          useLeft = false;
        }
      } else if (rightQueue.size() == 0 && leftQueue.size() == 0) {
        LOG.debug("No more Nodes in left or right queue. Stop BFS.");
        break;
      } else {
        useLeft = rightQueue.size() == 0;
      }
      if (useLeft) {
        curQueue = leftQueue;
        curVisited = leftVisited;
        leftLen++;
        curDistance = leftLen;
      } else {
        curQueue = rightQueue;
        curVisited = rightVisited;
        rightLen++;
        curDistance = rightLen;
      }
      LOG.debug("Length=" + curLen + ". Start to expand in "
          + (curQueue == rightQueue ? "RIGHT" : "LEFT") + " side. Queue Size=" + curQueue.size());

      List<List<ByteArray>> allRelatedNodes = concurrentExpand(curQueue);
      curQueue.clear();
      for (List<ByteArray> relatedNodes : allRelatedNodes) {
        totalDegree += relatedNodes.size();
        totalNode += 1;
        for (ByteArray bs : relatedNodes) {
          if (!curVisited.contains(bs)) {// not visited yet
            if (curDistance != pathLenMax) {// not the last hop for one side
              curQueue.add(bs);
            }
            // all fetched Nodes are marked "visited" even though they're not expanded
            curVisited.add(bs);
            NodeInfo info = nodesInfo.get(bs);
            if (info == null) {
              info = new NodeInfo();
              nodesInfo.put(bs, info);
            }
            if (useLeft) info.setLeftDis(curDistance);
            else info.setRightDis(curDistance);
          }
        }
      }
      //两点间可能是不连通的，
      if (curQueue.size() == 0 && curVisited.size() == 1) {// can not expand end node
        LOG.debug("No Edge for Node: " + curVisited.iterator().next() + ". Stop BFS.");
        break;
      }

      curLen++;
      LOG.debug("Length[ total=" + curLen + " left=" + leftLen + " right=" + rightLen + " ]"
          + "  BFS Tree size[ " + " left=" + leftVisited.size() + " right=" + rightVisited.size()
          + " ]" + "  Queue Size[ left=" + leftQueue.size() + " right=" + rightQueue.size() + " ]"
          + "  Avg Degree=" + (totalDegree * 1.0 / totalNode) + "  Avg HBase Req/s="
          + (totalHBaseReq.longValue() * 1.0 / Timer.secSince(start)));
    }

    /*
     * In the codes abode, we do not expand the last level for each side. So Nodes in the "middle"
     * of the graph may have no relatedNodes.
     * Consider: a path "start-A-B-C-D-E-end"
     * on left side: start->A->B->C, and start,A,B are expanded, C is not.
     * on right side: end->E->D->C, and end,E,D are expanded, C is not.
     * So, Node C will have no related Nodes.
     * 
     * Now, we will expand the "middle" Nodes below.
     */
    Set<ByteArray> intersectNodeSet = new HashSet<ByteArray>(leftVisited);
    intersectNodeSet.retainAll(rightVisited);
    // expand the Nodes shared by two sides
    concurrentExpand(new ArrayList<ByteArray>(intersectNodeSet));

    LOG.debug("HBase expand total time cost: " + Timer.msSince(start) + "ms");
    LOG.debug("HBase Req:" + totalHBaseReq.get() + " Time Per Req: "
        + (getHBaseReqTimeInMs() / totalHBaseReq.get()) + "ms");

    // Union two visited Set
    Set<ByteArray> nodeSet = new HashSet<ByteArray>(leftVisited);
    nodeSet.addAll(rightVisited);
    LOG.debug("BFS finished. start=" + Hex.encodeHexString(startId) + " end="
        + Hex.encodeHexString(endId) + "  BFS Tree size[ total=" + nodeSet.size() + " left="
        + leftVisited.size() + " right=" + rightVisited.size() + " ]");

    Preconditions.checkArgument(nodeSet.size() == nodesInfo.size(),
        "inconsistency in Node number. nodeSet=%s, nodeInfo=%s", nodeSet.size(), nodesInfo.size());

    /*
     * Phase 2: BFS without HBase data
     */
    // continue to BFS till each side's depth is maxPathLen with cached graph
    continueBfsWithCachedData(pathLenMax, leftLen, leftVisited, leftQueue, NodeInfo.LEFT_SIDE_IDX);
    continueBfsWithCachedData(pathLenMax, rightLen, rightVisited, rightQueue,
        NodeInfo.RIGHT_SIDE_IDX);

    /*
     * Phase 3: Purge#1
     * purge Nodes according to leftDis+rightDis<=maxPathLen
     */
    LOG.debug("Total Nodes before Purge#1: " + nodesInfo.size());
    Set<ByteArray> candidateNodes = new HashSet<ByteArray>(nodesInfo.size());
    for (Entry<ByteArray, NodeInfo> entry : nodesInfo.entrySet()) {
      if (entry.getValue().candidateNode(pathLenMax)) {//this is why not support directed edge
        candidateNodes.add(entry.getKey());
      }
    }
    LOG.debug("Nodes after Purge#1: " + candidateNodes.size());
    /*
     * Phase 4: Purge#2
     * Node degree except for start and end must not be one
     */
    Set<ByteArray> candidateNodes2 = new HashSet<ByteArray>(MapTool.capacityEstimate(candidateNodes
        .size()));
    Set<ByteArray> oneDegreeNodeSet = new HashSet<ByteArray>();
    for (ByteArray candidate : candidateNodes) {
      List<ByteArray> relatedNodes = relatedNodesCache.get(candidate);
      relatedNodes.retainAll(candidateNodes);// purge relatedNodes for later use
      if (candidate.equals(bfsStart) || candidate.equals(bfdEnd) || relatedNodes.size() != 1) {
        // degree of start and end Node can be 1
        candidateNodes2.add(candidate);
      } else {
        oneDegreeNodeSet.add(candidate);
      }
    }
    LOG.debug("Nodes after Purge#2: " + candidateNodes2.size());
    LOG.debug("One degree node size: " + oneDegreeNodeSet.size());

    /*
     * Phase 5: generate the PathGraph object
     */
    PathGraph graph = new PathGraph(startId, endId);
    for (ByteArray b : candidateNodes2) {
      List<ByteArray> related = relatedNodesCache.get(b);
      related.removeAll(oneDegreeNodeSet);
      // related.retainAll(candidateNodes2);    
      for (ByteArray re : related) {
        graph.addDirectedEdge(b.getBytes(), re.getBytes());
        graph.addDirectedEdge(re.getBytes(), b.getBytes());// the result graph is undirected.
      }
    }
    return graph;
  }

  private void continueBfsWithCachedData(int pathLenMax, int sideLen, Set<ByteArray> visited,
      ArrayList<ByteArray> queue, int sideIndex) {
    while (sideLen <= pathLenMax - 1 && queue.size() != 0) {
      sideLen++;
      List<ByteArray> toExpand = new ArrayList<ByteArray>(queue);
      queue.clear();
      for (ByteArray ba : toExpand) {
        List<ByteArray> related = relatedNodesCache.get(ba);// expand node by cache
        if (related == null) {
          // the Node is in the last level of one side and not in the intersection of visited[] of
          // two sides. It can not satisfy length requirements, just ignore it.
          continue;
        }
        for (ByteArray bs : related) {
          if (!visited.contains(bs)) {
            if (sideLen != pathLenMax) {
              // we don't need to save expanded node for the last hop
              queue.add(bs);
            }
            visited.add(bs);
            NodeInfo info = nodesInfo.get(bs);
            if (info != null) {
              info.set(sideIndex, sideLen);// get(bs) should never return null here
            } else {
              // some Nodes expanded from "middle" nodes will cause info==null
              // they are definitely out of path length limit, so ignore them
            }
          }
        }
      }
    }
  }

  /**
   * BFS from root with the given max depth.
   * 
   * @param root
   * @param bfsTreeMaxDepth
   * @return the Node Ids in the BFS tree
   */
  @Deprecated
  private Set<ByteArray> bfs(byte[] root, int bfsTreeMaxDepth) {
    LinkedList<ByteArray> queue = new LinkedList<ByteArray>();
    Set<ByteArray> visited = new HashSet<ByteArray>(30000);

    ByteArray bfsRoot = new ByteArray(root);
    queue.offer(bfsRoot);
    visited.add(bfsRoot);

    int curDepth = 0;
    long totalDegree = 0, totalNode = 0, totalHBaseReq = 0;
    long start = Timer.now();
    ByteArray curLevelEnd = queue.peek();
    while (curDepth <= bfsTreeMaxDepth - 1 && queue.size() != 0) {
      ByteArray curNode = queue.poll();
      List<ByteArray> x = expandNode(curNode);
      totalHBaseReq++;
      totalDegree += x.size();
      totalNode += 1;
      for (ByteArray bs : x) {
        if (!visited.contains(bs)) {
          if (curDepth != bfsTreeMaxDepth - 1) {
            queue.offer(bs);
          }
          visited.add(bs);
        }
      }
      if (curNode == curLevelEnd) {
        curDepth++;
        curLevelEnd = queue.peekLast();
        LOG.info("Level:" + curDepth + " BFS tree size=" + (visited.size())
            + "  Pending Queue Size=" + queue.size() + " Avg Degree="
            + (totalDegree * 1.0 / totalNode) + " HBase Req/s="
            + (totalHBaseReq * 1.0 / Timer.secSince(start)));
      }
    }
    LOG.info("BFS finished. root=" + Hex.encodeHexString(root) + "BFS Tree size=" + visited.size());
    return visited;
  }

  public static void main(String[] args) throws Exception {
    // c802a520757522343ee2c4a5c6ee0a703e36 //chengxueqi
    // c8024cc9c3ebb223208f86ef6706eed155b9//han jiawei
    // c8020189b6de2fb230485556d437fc155119 //wen jirong
    byte[] start = Hex.decodeHex("01084cc9c3ebb223208f86ef6706eed155b9".toCharArray());
    byte[] end = Hex.decodeHex("01080189b6de2fb230485556d437fc155119".toCharArray());

    int len = Integer.parseInt("3");
    PathQuerySpec spec = new PathQuerySpecBuilder(start, end).maxPathLength(len).build();
    // new GdbPathSearcher(spec, HBaseDAOFactory.getHBaseEdgeDAO()).searchForPathGraph();

    long startTs = Timer.now();
    GdbPathSearcher s = new GdbPathSearcher(spec, HBaseDAOFactory.getHBaseEdgeDAO(),
        Executors.newCachedThreadPool());
    s.search();
    LOG.info("Main returned in " + Timer.msSince(startTs) + "ms");
    System.exit(0);
  }
}
