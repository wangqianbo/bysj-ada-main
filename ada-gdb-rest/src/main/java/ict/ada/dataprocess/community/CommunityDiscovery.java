package ict.ada.dataprocess.community;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.util.OpenBitSet;
import org.apache.hadoop.hdfs.util.ByteArray;

/**
 * 最朴素的社区发现类 将所有边集按相似度排序，按类似Kruskal的算法进行自底向上的合并
 * 
 * @author fengzlzl
 * 
 */
/**
 * @author fengzlzl
 * 
 */
public class CommunityDiscovery {

  private static Log LOG = LogFactory.getLog(CommunityDiscovery.class);

  public static final int DEFAULT_MIN_COMMUNITY_NUMBER = 5;
  public static final double DEFAULT_MAX_COMMUNITY_DISTANCE = 0.9;
  public static final int MAX_BITSET_SIZE = 2000000;// 预设的最大url集合大小，与选的点数也有关，当前是选200个点
  public static final int DEFAULT_PEOPLE_SIZE = 200;// 预设的最大url集合大小，与选的点数也有关，当前是选200个点

  private Map<ByteArray, Collection<ByteArray>> nodeWtihUrls;
  private Map<ByteArray, Integer> nodeWithCommunityTag;
  private List<List<ByteArray>> Communities;
  private int numOfCommunity;

  private Map<ByteArray, Integer> urlHash;
  private List<OpenBitSet> bitSetList;
  private List<CommunityPair> pairs;
  private int[] parent;
  private Map<ByteArray, Integer> invertedIndex;

  private int minCommunityNumber;
  private double maxCommunityDistance;

  /**
   * 停止合并的的2个指标采用默认值 minCommunityNumber = 5 maxCommunityDistance = 0.5
   */
  public CommunityDiscovery() {
    minCommunityNumber = DEFAULT_MIN_COMMUNITY_NUMBER;
    maxCommunityDistance = DEFAULT_MAX_COMMUNITY_DISTANCE;
    initialzie();
    numOfCommunity = -1;
  }

  /**
   * 自底向上不断合并，停止合并的时机主要由以下2个指标决定：
   * 
   * @param minCommunityNumber
   *          最终结果中，至少有几个类的数目
   * @param maxCommunityDistance
   *          在合并过程中，能将2个类合并的能接受的最小距离,值域在[0,1]之间
   */
  public CommunityDiscovery(int minCommunityNumber, double maxCommunityDistance) {
    setMinCommunityNumber(minCommunityNumber);
    setMaxCommunityDistance(maxCommunityDistance);
    initialzie();
  }

  public void discoverCommunity(Map<ByteArray, Collection<ByteArray>> nodeWithUrls) {
    this.nodeWtihUrls = nodeWithUrls;
    generateBitsetLists();
    generateDistance();
    kruskal();
    generateTags();
  }

  public Map<ByteArray, Integer> getNodeWithCommunityTag() {
    return nodeWithCommunityTag;
  }

  public List<List<ByteArray>> getCommunities() {
    return Communities;
  }

  public int getNumOfCommunity() {
    return numOfCommunity;
  }

  private void kruskal() {
    long start = System.nanoTime();
    int communityNumber = bitSetList.size();
    parent = new int[communityNumber];
    for (int i = 0; i < communityNumber; i++)
      parent[i] = i;
    Collections.sort(pairs);
    for (CommunityPair pair : pairs) {
      // LOG.debug( "Distance " + pair.getDistance() + " between " + pair.getIndexA() + " and " +
      // pair.getIndexB());
      if (pair.getDistance() > maxCommunityDistance) break;
      int rootA = findRoot(pair.getIndexA());
      int rootB = findRoot(pair.getIndexB());
      if (rootA == rootB) continue;
      parent[rootB] = rootA;
      communityNumber--;
      if (communityNumber == minCommunityNumber) break;
    }
    long end = System.nanoTime();
    LOG.info("kruskal finished in " + ((end - start) * 1.0 / 1000000) + "ms");
  }

  private int findRoot(int index) {
    if (parent[index] == index) return index;
    return parent[index] = findRoot(parent[index]);
  }

  /**
   * 一边计算Url hash，一边计算bitset 预估出bitset的上界,预先申请处其大小的set上界 插入时bitset时使用fastset
   * 
   * @param graph
   */
  private void generateBitsetLists() {
    bitSetList.clear();
    long start = System.nanoTime();
    int estimageSize = 0;
    for (Entry<ByteArray, Collection<ByteArray>> node : nodeWtihUrls.entrySet()) {
      estimageSize += node.getValue().size();
    }

    if (estimageSize > MAX_BITSET_SIZE) estimageSize = MAX_BITSET_SIZE;
    urlHash = new HashMap<ByteArray, Integer>(estimageSize);
    int maxIndex = 0;
    for (Entry<ByteArray, Collection<ByteArray>> node : nodeWtihUrls.entrySet()) {
      // 根据边上的urllist构造该对应点的url集合
      // OpenBitSet set = new OpenBitSet(estimageSize);
      Collection<ByteArray> urlCollection = node.getValue();
      OpenBitSet set = new OpenBitSet(maxIndex);
      for (ByteArray urlbyte : urlCollection) {
        Integer bitIndex = urlHash.get(urlbyte);
        if (bitIndex == null) {
          bitIndex = urlHash.size();
          urlHash.put(urlbyte, bitIndex);
        }
        set.set(bitIndex);
        if (bitIndex > maxIndex) {
          maxIndex = bitIndex;
        }
      }
      bitSetList.add(set);
    }
    LOG.info(" URL hash size : " + urlHash.size());
    long end = System.nanoTime();
    LOG.info("Generating url sets finished in " + ((end - start) * 1.0 / 1000000) + "ms");
  }

  private void initialzie() {
    bitSetList = new ArrayList<OpenBitSet>(DEFAULT_PEOPLE_SIZE);
    pairs = new ArrayList<CommunityPair>(DEFAULT_PEOPLE_SIZE * (DEFAULT_PEOPLE_SIZE - 1) / 2);
    invertedIndex = new HashMap<ByteArray, Integer>(DEFAULT_PEOPLE_SIZE);
    Communities = new ArrayList<List<ByteArray>>();
    nodeWithCommunityTag = new HashMap<ByteArray, Integer>();
  }

  private void generateDistance() {
    long start = System.nanoTime();
    pairs.clear();
    for (int i = 0; i < bitSetList.size(); i++) {
      for (int j = i + 1; j < bitSetList.size(); j++) {
        double distance = calculateDistance(bitSetList.get(i), bitSetList.get(j));
        pairs.add(new CommunityPair(i, j, distance));
      }
    }
    long end = System.nanoTime();
    LOG.info("Calcluating distacne between url sets finished in " + ((end - start) * 1.0 / 1000000)
        + "ms");
  }

  private void generateTags() {
    if (nodeWithCommunityTag != null) this.nodeWithCommunityTag.clear();
    if (this.Communities != null) this.Communities.clear();
    int i = 0;
    Map<Integer, Integer> tags = new HashMap<Integer, Integer>(nodeWtihUrls.size());
    for (Entry<ByteArray, Collection<ByteArray>> node : nodeWtihUrls.entrySet()) {
      int root = findRoot(i++);
      Integer tag;
      if (tags.containsKey(root)) {
        tag = tags.get(root);
      } else {
        tag = tags.size();
        tags.put(root, tag);
        this.Communities.add(new ArrayList<ByteArray>());
      }
      this.nodeWithCommunityTag.put(node.getKey(), tag);
      this.Communities.get(tag).add(node.getKey());
    }
    this.numOfCommunity = tags.size();
    LOG.info("Total " + tags.size() + " community is discovered.");
  }

  final private double calculateDistance(OpenBitSet setA, OpenBitSet setB) {
    // Jaccard similarity coefficient
    double coefficient = OpenBitSet.intersectionCount(setA, setB)
        / (double) OpenBitSet.unionCount(setA, setB);
    double distance = 1.0 - coefficient;
    return distance;
  }

  public int getMinCommunityNumber() {
    return minCommunityNumber;
  }

  public void setMinCommunityNumber(int minCommunityNumber) {
    this.minCommunityNumber = minCommunityNumber;
  }

  public double getMaxCommunityDistance() {
    return maxCommunityDistance;
  }

  public void setMaxCommunityDistance(double maxCommunityDistance) {
    this.maxCommunityDistance = maxCommunityDistance;
  }

}
