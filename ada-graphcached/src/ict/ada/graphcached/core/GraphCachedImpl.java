/**
 * 
 */
package ict.ada.graphcached.core;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ict.ada.graphcached.conf.GraphCachedRelationshipType;
import ict.ada.graphcached.model.CachedNode;
import ict.ada.graphcached.model.CachedRelationship;
import ict.ada.graphcached.util.Pair;
import ict.ada.graphcached.util.Triplet;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.Traversal;

/**
 * @author forhappy
 * 
 */
public class GraphCachedImpl {

  public final static String NODE_KEY = "id";
  public final static String RELATIONSHIP_KEY = "id";
  public final static String DB_PATH = "data/";

  private GraphDatabaseService graphDb;
  private Index<Node> nodeIndex;
  private Index<Relationship> relationIndex;

  public GraphCachedImpl() {
    super();
    graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
    nodeIndex = graphDb.index().forNodes("nodes");
    relationIndex = graphDb.index().forRelationships("relationships");
    registerShutdownHook();
  }

  public GraphCachedImpl(String path) {
    super();
    graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(path);
    nodeIndex = graphDb.index().forNodes("nodes");
    relationIndex = graphDb.index().forRelationships("relationships");
    registerShutdownHook();
  }

  public Node addNode(CachedNode cachedNode) {
    Node node = null;
    Transaction tx = graphDb.beginTx();
    try {
      node = graphDb.createNode();
      node.setProperty(NODE_KEY, cachedNode.getNodeId());
      nodeIndex.add(node, NODE_KEY, cachedNode.getNodeId());
      tx.success();
    } finally {
      tx.finish();
    }

    return node;
  }

  public Node addNodeIfNotExits(CachedNode cachedNode) {
    Node node = null;
    if (nodeExists(cachedNode) == true)
      return findNode(cachedNode);
    else
      node = addNode(cachedNode);
    return node;
  }

  public boolean nodeExists(CachedNode cachedNode) {
    if (nodeIndex.get(NODE_KEY, cachedNode.getNodeId()).getSingle() != null)
      return true;
    return false;
  }

  public Node findNode(CachedNode cachedNode) {
    Node found = nodeIndex.get(NODE_KEY, cachedNode.getNodeId()).getSingle();
    if (found != null)
      return found;
    return null;
  }

  public boolean relationshipExists(CachedRelationship cachedRelationship) {
    if (relationIndex.get(RELATIONSHIP_KEY, cachedRelationship.getRelationshipId()).getSingle() != null)
      return true;
    return false;
  }

  public Relationship findRelationship(CachedRelationship cachedRelationship) {
    Relationship found = relationIndex
        .get(RELATIONSHIP_KEY, cachedRelationship.getRelationshipId()).getSingle();
    if (found != null)
      return found;
    return null;
  }

  public void addRelationship(CachedNode start, CachedNode end, CachedRelationship relationship) {

    Node startNode = null;
    Node endNode = null;
    Relationship relation = null;

    Transaction tx = graphDb.beginTx();

    try {
      startNode = addNodeIfNotExits(start);
      endNode = addNodeIfNotExits(end);
      if (!relationshipExists(relationship)) {
        relation = startNode.createRelationshipTo(endNode, GraphCachedRelationshipType.CONNECTED);
        relation.setProperty(RELATIONSHIP_KEY, relationship.getRelationshipId());
        relationIndex.add(relation, RELATIONSHIP_KEY, relationship.getRelationshipId());
      }
      
      tx.success();
    } finally {
      tx.finish();
    }
  }

  public void addRelationships(List<Triplet<CachedNode, CachedNode, CachedRelationship>> relations) {
    Node startNode = null;
    Node endNode = null;
    Relationship relation = null;
    int txCount = 0, totalLeft = relations.size();

    Iterator<Triplet<CachedNode, CachedNode, CachedRelationship>> iter = relations.iterator();
    Transaction tx = graphDb.beginTx();
    try {
      while (iter.hasNext()) {
        Triplet<CachedNode, CachedNode, CachedRelationship> triplet =
            (Triplet<CachedNode, CachedNode, CachedRelationship>) iter.next();
        if (txCount >= 300000) {
          tx.success();
          tx.finish();
          totalLeft -= txCount;
          System.out.println(totalLeft + " relationships are left to import, please wait.");
          txCount = 0;
          tx = graphDb.beginTx();
        }
        startNode = addNodeIfNotExits(triplet.getFirst());
        endNode = addNodeIfNotExits(triplet.getSecond());
        if (!relationshipExists(triplet.getThird())) {
          relation = startNode.createRelationshipTo(endNode, GraphCachedRelationshipType.CONNECTED);
          relation.setProperty(RELATIONSHIP_KEY, triplet.getThird().getRelationshipId());
          relationIndex.add(relation, RELATIONSHIP_KEY, triplet.getThird().getRelationshipId());
        }
        txCount += 1;
      }
      tx.success();
    } finally {
      tx.finish();
    }
  }
  
  public void addRelationships(Map<CachedNode, List<Pair<CachedNode, CachedRelationship>>> relations) {
    Node startNode = null;
    Node endNode = null;
    Relationship relation = null;
    int txCount = 0;
    
    Iterator<Entry<CachedNode, List<Pair<CachedNode, CachedRelationship>>>> it = 
        relations.entrySet().iterator();
    Transaction tx = graphDb.beginTx();
    try {
      while (it.hasNext()) {
        if (txCount > 10000) {
          tx.success();
          tx.finish();
          txCount = 0;
          tx = graphDb.beginTx();
        }
        Map.Entry<CachedNode, List<Pair<CachedNode, CachedRelationship>>> entry = it.next();
        CachedNode start = entry.getKey();
        List<Pair<CachedNode, CachedRelationship>> endAndRelations = entry.getValue();
        startNode = addNodeIfNotExits(start);
        for (Pair<CachedNode, CachedRelationship> pair: endAndRelations) {
          CachedNode end = (CachedNode) pair.getFirst();
          CachedRelationship r = (CachedRelationship) pair.getSecond();
          endNode = addNodeIfNotExits(end);
          if (!relationshipExists(r)) {
            relation = startNode.createRelationshipTo(endNode, GraphCachedRelationshipType.CONNECTED);
            relation.setProperty(RELATIONSHIP_KEY, r.getRelationshipId());
            relationIndex.add(relation, RELATIONSHIP_KEY, r.getRelationshipId());
          }
        }
        txCount += 1;
      }
      tx.success();
    } finally {
      tx.finish();
    }
  }
  
  public Iterable<Path> findShortestPath(CachedNode start, CachedNode end) {
    Node startNode = findNode(start);
    Node endNode = findNode(end);
    if (startNode == null || endNode == null) return null;
    Iterable<Path> paths = findShortestPath(startNode, endNode);
    return paths;
  }
  
  private Iterable<Path> findShortestPath(Node startNode, Node endNode) {
    PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
        Traversal.expanderForTypes(GraphCachedRelationshipType.CONNECTED, Direction.BOTH), 64);
    Iterable<Path> paths = finder.findAllPaths(startNode, endNode);
    return paths;
  }
  
  public Iterable<Path> allPaths(CachedNode start, CachedNode end) {
    Node startNode = findNode(start);
    Node endNode = findNode(end);
    if (startNode == null || endNode == null) return null;
    Iterable<Path> paths = allPaths(startNode, endNode);
    return paths;
  }
  
  private Iterable<Path> allPaths(Node startNode, Node endNode) {
    PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
        Traversal.expanderForTypes(GraphCachedRelationshipType.CONNECTED, Direction.BOTH), 64);
    Iterable<Path> paths = finder.findAllPaths(startNode, endNode);
    return paths;
  }

  public Iterable<Path> allSimplePaths(CachedNode start, CachedNode end) {
    Node startNode = findNode(start);
    Node endNode = findNode(end);
    if (startNode == null || endNode == null) return null;
    Iterable<Path> paths = allSimplePaths(startNode, endNode);
    return paths;
  }
  
  private Iterable<Path> allSimplePaths(Node startNode, Node endNode) {
    PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
        Traversal.expanderForTypes(GraphCachedRelationshipType.CONNECTED, Direction.BOTH), 64);
    Iterable<Path> paths = finder.findAllPaths(startNode, endNode);
    return paths;
  }
  
  private void registerShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        graphDb.shutdown();
      }
    });
  }

}
