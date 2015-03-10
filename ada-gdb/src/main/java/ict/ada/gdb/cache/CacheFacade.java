package ict.ada.gdb.cache;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.RelationGraph;
import ict.ada.common.model.RelationType;
import ict.ada.common.util.Hex;
import ict.ada.common.util.MapTool;
import ict.ada.common.util.Timer;
import ict.ada.gdb.cache.GdbCacheProto.CacheExploreSpec;
import ict.ada.gdb.cache.GdbCacheProto.EdgeList;
import ict.ada.gdb.cache.GdbCacheProto.StatusPingResult;
import ict.ada.gdb.common.AdaConfig;
import ict.ada.gdb.common.PathQuerySpec;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.common.RelQuerySpec.RelQuerySpecBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.zeromq.ZMQ;

import com.google.protobuf.InvalidProtocolBufferException;

public class CacheFacade {

  private static final ZMQ.Context zmqContext = ZMQ.context(1);
  private static SocketFactory socketFactory;

  private static Map<Integer, SocketPool> socketPool;// hash value => pool
  private static final int POOL_SIZE_MAX = 150;

  private final int gdbCacheClusterSize;
  private final String[] cacheNodes;

  private CacheFacade() {
    if (!AdaConfig.ENABLE_GDB_CACHE) {
      throw new IllegalStateException("AdaConfig.ENABLE_GDB_CACHE=false");
    }
    if (AdaConfig.GDB_CACHE_ENTRY_ADDRESS == null) {
      throw new RuntimeException(
          "GDB Cache is enabled, but GDB Cache 'entry address' is not provided in GDB configuration file.");
    }
    // TODO ask zk for Cache machines
    cacheNodes = requestClusterMembers();
    if (cacheNodes == null) {
      throw new RuntimeException("GDB Cache is not available");
    }
    gdbCacheClusterSize = cacheNodes.length;
    socketFactory = new SocketFactory(cacheNodes);
    Map<Integer, SocketPool> map = MapTool.newHashMap();
    for (int hash = 0; hash < cacheNodes.length; hash++) {
      SocketPool p = new SocketPool(hash, POOL_SIZE_MAX);// one Socket pool for each address
      map.put(hash, p);
    }
    socketPool = Collections.unmodifiableMap(map);
  }

  public String[] getCacheNodesAddrs() {
    return cacheNodes;
  }

  /** Singleton */
  private static CacheFacade INSTANCE;

  public static synchronized CacheFacade get() {
    if (INSTANCE == null) INSTANCE = new CacheFacade();
    return INSTANCE;
  }

  private static class SocketPool {
    private Integer poolId;
    private int poolMaxSize;
    private ConcurrentLinkedQueue<ZMQ.Socket> pool = new ConcurrentLinkedQueue<ZMQ.Socket>();

    public SocketPool(Integer poolId, int maxSize) {
      this.poolId = poolId;
      this.poolMaxSize = maxSize;
      pool.offer(socketFactory.createReqSocket(poolId));
    }

    public ZMQ.Socket get() {
      ZMQ.Socket sock = pool.poll();
      return sock == null ? socketFactory.createReqSocket(poolId) : sock;
    }

    public void put(ZMQ.Socket sock) {
      if (sock == null) return;
      if (pool.size() < poolMaxSize) {
        pool.offer(sock);
      } else {
        sock.close();
      }
    }
  }

  private static class SocketFactory {
    final String[] cacheNodesAddr;

    public SocketFactory(String[] cacheNodeAddrs) {
      this.cacheNodesAddr = cacheNodeAddrs;
    }

    public ZMQ.Socket createReqSocket(Integer poolId) {
      String addr = cacheNodesAddr[poolId];
      ZMQ.Socket socket = zmqContext.socket(ZMQ.REQ);
      // socket.setSendTimeOut(50);// TODO
      // TODO receive timeout; Used to avoid slow query and cope with cache node crash
      socket.setReceiveTimeOut(ZMQ_RECV_TIMEOUT_MS);
      socket.connect("tcp://" + addr);
      return socket;
    }
  }

  private static class ZmqSendException extends Exception {
  }

  private static class ZmqRecvException extends Exception {
  }

  private int determineCacheSiteId(byte[] nodeid) {
    // TODO route request according to Cache machines in Zookeeper
    // MUST be consistent with hashing in CacheServer
    if (nodeid == null) return 0;
    int result = 1;
    for (byte element : nodeid) {
      result = 31 * result + element;
    }
    return (result % gdbCacheClusterSize + gdbCacheClusterSize) % gdbCacheClusterSize;
  }

  private ZMQ.Socket getNewReqSocketByAddress(String ipWithPort) {
    ZMQ.Socket sock = zmqContext.socket(ZMQ.REQ);
    sock.setReceiveTimeOut(ZMQ_RECV_TIMEOUT_MS);
    sock.connect("tcp://" + ipWithPort);
    return sock;
  }

  private byte[] rpcCall(ZMQ.Socket sock, byte[] req, String methodName) throws ZmqSendException,
      ZmqRecvException {
    if (!sock.send(req, ZMQ.DONTWAIT)) {
      System.err.println("ZMQ send() returns false. method=[" + methodName + "]");
      throw new ZmqSendException();// fail to send message in time.
    }
    byte[] response = sock.recv();// block wait
    if (response == null) {
      System.err.println("ZMQ recv() returns null( may be timeout). method=[" + methodName + "]");
      throw new ZmqRecvException();
    }
    return response;
  }

  private static final int ZMQ_RECV_TIMEOUT_MS = 1000;// TODO receive timeout

  /*
   * ========== a couple of RPCs below =============
   */

  /*
   * ======== Cluster Members RPC
   */
  private static final byte METHOD_ID_CLUSTER_MEMBERS = 30;
  private static final byte[] CLUSTER_MEMBERS_REQ = { METHOD_ID_CLUSTER_MEMBERS };

  /** GDB client will request this address for cache cluster members' info */
  private static final String ENTRY_ADDRESS = AdaConfig.GDB_CACHE_ENTRY_ADDRESS;

  /**
   * @return cache sites' addresses, ip:port
   */
  public String[] requestClusterMembers() {
    ZMQ.Socket sock = getNewReqSocketByAddress(ENTRY_ADDRESS);
    System.out.println("Requesting " + ENTRY_ADDRESS + " for cache cluster members' info...");
    try {
      byte[] response;
      try {
        response = rpcCall(sock, CLUSTER_MEMBERS_REQ, "requestClusterMembers");
      } catch (ZmqSendException e1) {
        return null;
      } catch (ZmqRecvException e1) {
        return null;
      }
      String resp = new String(response);
      System.out.println("=== Cache Cluster Members Info ===\n"
          + "[Hash ID]\t[Server Address]\t[Server Tag]\n" + resp);
      Map<Integer, String> idToAddr = new HashMap<Integer, String>();
      Scanner cin = new Scanner(resp);
      while (cin.hasNext()) {
        int id = cin.nextInt();
        String addr = cin.next();
        String tag = cin.next();
        idToAddr.put(id, addr);
      }
      cin.close();
      String[] sites = new String[idToAddr.size()];
      for (Entry<Integer, String> entry : idToAddr.entrySet()) {
        sites[entry.getKey()] = entry.getValue();
      }
      return sites;
    } finally {
      sock.close();
    }
  }

  /*
   * ======== Status Ping RPC
   */
  private static final byte METHOD_ID_STATUS_PING = 20;
  private static final byte[] STATUS_PING_REQ = { METHOD_ID_STATUS_PING };

  public void requestAndPrintAllStatus() {
    for (String site : cacheNodes) {
      String status = requestCacheSiteStatusString(site);
      System.out.println(status);
    }
  }

  private static final String ONLINE_TAG = "ONLINE";

  private String requestCacheSiteStatusString(String address) {
    StatusPingResult result;
    try {
      result = requestCacheSiteStatus(address);
    } catch (ZmqSendException e1) {
      return "[" + address + "] " + "RPC_ERROR";
    } catch (ZmqRecvException e1) {
      return "[" + address + "] " + "OFFLINE";
    }
    long dbSize = result.getDbSize();
    long kvCount = result.getKvCount();
    boolean cacheOpInProgress = result.getCacheOpInProgress();
    return "[" + address + "] " + ONLINE_TAG + " { db_bytes= " + dbSize + " kv_count= " + kvCount
        + " cache_op_in_progress= " + cacheOpInProgress + " }";
  }

  private StatusPingResult requestCacheSiteStatus(String address) throws ZmqSendException,
      ZmqRecvException {
    ZMQ.Socket sock = getNewReqSocketByAddress(address);
    System.out.println("Requesting status of " + address + "...");
    try {
      byte[] response = rpcCall(sock, STATUS_PING_REQ, "requestCacheSiteStatus");
      try {
        StatusPingResult result = StatusPingResult.parseFrom(response);
        return result;
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Invalid StatusPingResult PB message", e);
      }
    } finally {
      sock.close();
    }
  }

  private boolean isCacheSiteReady(String address) {
    StatusPingResult result;
    try {
      result = requestCacheSiteStatus(address);
    } catch (ZmqSendException e) {
      return false;
    } catch (ZmqRecvException e) {
      return false;
    }
    return !result.getCacheOpInProgress();
  }

  /*
   * ======== Load/Delete Cache RPC
   */

  private static final byte METHOD_ID_LOAD_DATA_IN_FILE = 40;
  private static final byte METHOD_ID_DELETE_DATA_IN_FILE = 41;
  private static final byte BOOLEAN_TRUE = '1';
  private static final byte BOOLEAN_FALSE = '0';

  private boolean submitNodeIdFileToManipulateCache(String methodName, byte methodId,
      String ipWithPort, String nodeIdFilePath, boolean localFs) {
    byte[] req = buildRpcReqBytes(nodeIdFilePath, localFs, methodId);
    ZMQ.Socket sock = getNewReqSocketByAddress(ipWithPort);
    try {
      byte[] response;
      try {
        response = rpcCall(sock, req, methodName);
      } catch (ZmqSendException e) {
        return false;
      } catch (ZmqRecvException e) {
        return false;
      }
      if (response.length != 1)
        throw new RuntimeException("Should not happen. rpc response size is not 1.");
      if (response[0] == BOOLEAN_TRUE) return true;
      else if (response[0] == BOOLEAN_FALSE) return false;
      else throw new RuntimeException("Invalid response value: " + response[0]);
    } finally {
      sock.close();
    }
  }

  private boolean submitNodeIdFileToClusterToManipulateCache(String methodName, byte methodId,
      String nodeIdFilePath, boolean localFs) {
    String sepLine = "===============";
    System.out
        .println("Pinging all CacheServers to ensure that no cache operation is in progress......");
    System.out.println(sepLine);
    for (String cacheSite : cacheNodes) {
      if (!isCacheSiteReady(cacheSite)) {
        System.out.println("[" + cacheSite + "] is NOT ready.");
        System.out.println(sepLine);
        System.out
            .println("Please ensure that no cache operation is in progress in GdbCache cluster.");
        return false;
      } else {
        System.out.println("[" + cacheSite + "] is READY");
      }
    }
    System.out.println(sepLine + "\n");

    System.out.println("Starting to submit tasks to CacheServers......");
    System.out.println(sepLine);

    int ok = 0, fail = 0;
    for (String cacheSite : cacheNodes) {
      boolean success = submitNodeIdFileToManipulateCache(methodName, methodId, cacheSite,
          nodeIdFilePath, localFs);
      System.out.println("[" + cacheSite + "] " + (success ? "TASK ACCEPTED" : "FAILURE"));
      if (success) ok++;
      else fail++;
    }
    System.out.println(sepLine);
    System.out.println("TASK ACCEPTED= " + ok + " FAILURE= " + fail);
    return fail == 0 ? true : false;
  }

  /**
   * 
   * Ask GdbCache to LOAD data in a given file and return immediately(non-blocking)
   * 
   * @param nodeIdFilePath
   *          without FileSystem-type prefix, e.g. /ada/nodes.id
   * @param localFs
   *          true for local file system, false for HDFS
   * @return
   *         true if the task submitted successfully.
   */
  public boolean submitNodeIdFileToLoadCache(String ipWithPort, String nodeIdFilePath,
      boolean localFs) {
    return submitNodeIdFileToManipulateCache("submitNodeIdFileToLoadCache",
        METHOD_ID_LOAD_DATA_IN_FILE, ipWithPort, nodeIdFilePath, localFs);
  }

  public boolean submitNodeIdFileToClusterToLoadCache(String nodeIdFilePath, boolean localFs) {
    return submitNodeIdFileToClusterToManipulateCache("submitNodeIdFileToClusterToLoadCache",
        METHOD_ID_LOAD_DATA_IN_FILE, nodeIdFilePath, localFs);

  }

  public boolean submitNodeIdFileToDeleteCache(String ipWithPort, String nodeIdFilePath,
      boolean localFs) {
    return submitNodeIdFileToManipulateCache("submitNodeIdFileToDeleteCache",
        METHOD_ID_DELETE_DATA_IN_FILE, ipWithPort, nodeIdFilePath, localFs);
  }

  public boolean submitNodeIdFileToClusterToDeleteCache(String nodeIdFilePath, boolean localFs) {
    return submitNodeIdFileToClusterToManipulateCache("submitNodeIdFileToClusterToDeleteCache",
        METHOD_ID_DELETE_DATA_IN_FILE, nodeIdFilePath, localFs);
  }

  private byte[] buildRpcReqBytes(String filePath, boolean localFs, byte methodId) {
    byte[] pathBytes = filePath.getBytes();
    byte[] req = new byte[pathBytes.length + 1 + 1];// + boolean byte + method id byte
    System.arraycopy(pathBytes, 0, req, 0, pathBytes.length);
    req[pathBytes.length] = localFs ? BOOLEAN_TRUE : BOOLEAN_FALSE;
    req[pathBytes.length + 1] = methodId;
    return req;
  }

  /*
   * ======== RelQuery RPC
   * 
   * request format:
   * |---CacheExploreSpec PB bytes---|-- method id 1B--|
   * 
   * RelQuery response format:
   * |-- EdgeList --| (normal)
   * |- cache miss code 1B -| (cache miss)
   * 
   * MUST be consistent with GdbCacheServer c++ code
   */
  private static final byte METHOD_ID_REL_QUERY = 10;
  private static final byte RETURN_CODE_REL_QUERY_CACHE_MISS = 99;

  /*
   * ATTENTION: Strictly speaking, jzmq socket is not thread safe and we should not pool them.
   * However, documents say that they can be used in multiple threads if and only if a memory
   * barrier is used.
   * We "suppose" necessary memory barrier is applied by 'volatile' access in socketPool's
   * ConcurrentLinkedQueue.
   */

  /**
   * Try to query RelationGraph result from GdbCache
   * 
   * @param relQuerySpec
   * @param loadCacheOnMiss
   *          Whether to instruct GdbCache to load cache data on miss.
   *          If true, returned RelationGraph "should" not be NULL.
   *          If false, returned RelationGraph will be NULL on cache miss.
   * @return
   */
  public RelationGraph queryRelationGraph(RelQuerySpec relQuerySpec, boolean loadCacheOnMiss) {
    CacheExploreSpec cacheSpec = ProtobufAdapter.toProtobuf(relQuerySpec, loadCacheOnMiss);
    EdgeList rawEdgeList = doCacheExplore(cacheSpec);
    if (rawEdgeList == null) return null;// should not happen
    return ProtobufAdapter.convertEdgeListPB(relQuerySpec, rawEdgeList);
  }

  /**
   * equals queryRelationGraph(spec, false);
   */
  public RelationGraph queryRelationGraph(RelQuerySpec spec) {
    return queryRelationGraph(spec, false);
  }

  /**
   * For PathGraphQuery
   */
  public List<byte[]> queryRelatedNodeIdList(Node nodeToExpand, PathQuerySpec pathSpec) {
    CacheExploreSpec cacheSpec = ProtobufAdapter.toProtobuf(nodeToExpand, pathSpec, false);
    EdgeList rawEdgeList = doCacheExplore(cacheSpec);
    if (rawEdgeList == null) return null;// should not happen
    return ProtobufAdapter.convertEdgeListPB(nodeToExpand, pathSpec, rawEdgeList);
  }

  private byte[] getReqBytesFromCacheExploreSpec(CacheExploreSpec spec) {
    byte[] specPbBytes = spec.toByteArray();
    byte[] req = new byte[specPbBytes.length + 1];
    System.arraycopy(specPbBytes, 0, req, 0, specPbBytes.length);
    req[req.length - 1] = METHOD_ID_REL_QUERY;
    // System.out.println("RPC Request size=" + req.length);
    return req;
  }

  private EdgeList doCacheExplore(CacheExploreSpec spec) {
    byte[] nodeId = spec.getCenterId().toByteArray();
    SocketPool sockPool = socketPool.get(determineCacheSiteId(nodeId));
    ZMQ.Socket sock = sockPool.get();// an implicit memory barrier is supposed here
    try {
      byte[] response;
      try {
        response = rpcCall(sock, getReqBytesFromCacheExploreSpec(spec), "doCacheExplore()");
      } catch (ZmqSendException e1) {
        return null;
      } catch (ZmqRecvException e1) {
        sock.close();// close it because reusing will cause REQ-REP semantic violated
        sock = null;// do not put back into pool
        return null;
      }
      if (response.length == 1) {// cache miss
        if (response[0] != RETURN_CODE_REL_QUERY_CACHE_MISS)
          throw new IllegalStateException("Unknown return code:" + response[0]);
        // System.out.println("Cache miss");
        return null;
      }
      EdgeList edgeListPb;
      try {
        edgeListPb = EdgeList.parseFrom(response);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Invalid EdgeList PB message", e);
      }
      return edgeListPb;
    } finally {
      sockPool.put(sock);// reuse sock; if sock=null, no effect
    }
  }

  /*
   * Unit Test
   */

  private static RelationGraph testRelQuerys(RelQuerySpec spec, String testName) {
    long start = Timer.now();
    RelationGraph relGraph = CacheFacade.get().queryRelationGraph(spec);
    System.out.println(relGraph == null ? null : relGraph.toDetailedString());
    System.out.println(testName + ". Time cost: " + Timer.msSince(start) + "ms");
    return relGraph;
  }

  private static void printAssert(boolean resullt) {
    System.out.println(resullt ? "OK" : "!!!!!!!!!!!!!!FAILURE!!!!!!!!!!!");
  }

  private static void testBody() {
    final String xijinping = "01331fa2bf8c04ffcaf2a134736add6150c6";
    // testRelQuery(xijinping);
    // testRelQuery(xijinping);
    // testRelQuery(xijinping);
    // testRelQuery(xijinping);
    // testRelQuery(xijinping);
    RelationGraph relG;
    String node = xijinping;
    RelQuerySpec spec0 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(node))).build();
    relG = testRelQuerys(spec0, "No constrains");
    printAssert(relG.getCenterEdges().size() == 26);

    RelQuerySpec spec1 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(node))).attribute(
        Attribute.ORG).build();
    relG = testRelQuerys(spec1, "NodeType constrains");
    printAssert(relG.getCenterEdges().size() == 1);

    RelQuerySpec spec2 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(node)))
        .attribute(Attribute.ORG).relType(RelationType.getType("属于")).build();
    relG = testRelQuerys(spec2, "NodeType RelType constrains");
    printAssert(relG.getCenterEdges().size() == 0);

    RelQuerySpec spec21 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(node)))
        .attribute(Attribute.CATEGORY).relType(RelationType.getType("属于")).build();
    relG = testRelQuerys(spec21, "NodeType RelType constrains");
    printAssert(relG.getCenterEdges().size() == 25);

    RelQuerySpec spec3 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(node)))
        .attribute(Attribute.CATEGORY).relType(RelationType.getType("属于")).resultSize(15).build();
    relG = testRelQuerys(spec3, "NodeType RelType ResultSize constrains");
    printAssert(relG.getCenterEdges().size() == 15);

    RelQuerySpec spec4 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(node)))
        .attribute(Attribute.CATEGORY).relType(RelationType.getType("属于")).resultSize(15)
        .useRelRank(true).build();
    relG = testRelQuerys(spec4, "NodeType RelType ResultSize useRank constrains");
    printAssert(relG.getCenterEdges().size() == 15);

    RelQuerySpec spec5 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(node))).minWeight(2)
        .build();
    relG = testRelQuerys(spec5, "Min weight constrains");
    printAssert(relG.getCenterEdges().size() == 0);

    RelQuerySpec spec6 = new RelQuerySpecBuilder(new Node(Hex.decodeHex(node))).minWeight(1)
        .maxWeight(1).resultSize(5).build();
    relG = testRelQuerys(spec6, "Min/Max result size weight constrains");
    printAssert(relG.getCenterEdges().size() == 5);
  }

  public static void main(String[] args) throws InterruptedException {
    ExecutorService exec = Executors.newCachedThreadPool();
    int threads = args.length > 0 ? Integer.parseInt(args[0]) : 0;
    for (int i = 0; i < threads; i++) {
      exec.execute(new Runnable() {
        @Override
        public void run() {
          testBody();
        }
      });
      Thread.sleep(50);
    }
    exec.shutdown();
    exec.awaitTermination(10000, TimeUnit.SECONDS);
    System.out.println("Single Thread Test");
    testBody();
  }
}
