/**
 * 
 */
package ict.ada.graphcached.process;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType;
import ict.ada.common.util.Hex;
import ict.ada.graphcached.ada.EdgeIdGenerator;
import ict.ada.graphcached.ada.NodeIdGenerator;
import ict.ada.graphcached.core.GraphReader;
import ict.ada.graphcached.model.CachedNode;
import ict.ada.graphcached.model.CachedRelationship;
import ict.ada.graphcached.util.GzipFileReader;
import ict.ada.graphcached.util.Pair;
import ict.ada.graphcached.util.StringUtil;
import ict.ada.graphcached.util.Triplet;

public class ScholarGraphReader implements GraphReader {
  
  private final String SCHOLAR_GRAPH_DUMMY_NAME = "X"; // YOU SHOULD NOT USE THIS IN YOUR CODE.

  @Override
  public List<Triplet<CachedNode, CachedNode, CachedRelationship>> read(String file) {
    List<Triplet<CachedNode, CachedNode, CachedRelationship>> relationshipList = new ArrayList<Triplet<CachedNode, CachedNode, CachedRelationship>>();
    BufferedReader br = null;
    String line = null;
    String nA, nB, startNodeId, endNodeId, relationId;
    Node startNode = new Node(NodeType.SCHOLAR_PERSON, SCHOLAR_GRAPH_DUMMY_NAME);
    Node endNode = new Node(NodeType.SCHOLAR_PERSON, SCHOLAR_GRAPH_DUMMY_NAME);
    try {
      br = GzipFileReader.getFileReader(file);
      while ((line = br.readLine()) != null) {
        nA = StringUtil.GetBetween(line, "\"nA\": \"", "\",");
        nB = StringUtil.GetBetween(line, "\"nB\": \"", "\",");
        if (nA == null || nB == null) {
          System.out.println("Start Node: " + nA + "\tEnd Node: " + nB);
          continue;
        }
        
        startNode.setNameAndType(nA, NodeType.SCHOLAR_PERSON);
        endNode.setNameAndType(nB, NodeType.SCHOLAR_PERSON);
        
        // System.out.println("Start Node: " + startNode.getName() + "\nEnd Node: " + endNode.getName());
        // You have to call NodeIdGenerator.generate(Node node) first to get its node id 
        // before calling EdgeIdGenerator.generate(edge).
        startNodeId = Hex.encodeHex(NodeIdGenerator.generate(startNode));
        endNodeId = Hex.encodeHex(NodeIdGenerator.generate(endNode));
        
        Edge edge = new Edge(startNode, endNode);
        CachedNode start = new CachedNode(startNodeId);
        CachedNode end = new CachedNode(endNodeId);
        relationId = Hex.encodeHex(EdgeIdGenerator.generate(edge));
        CachedRelationship relationship = new CachedRelationship(relationId);

        Triplet<CachedNode, CachedNode, CachedRelationship> triplet = new Triplet<CachedNode, CachedNode, CachedRelationship>(
            start, end, relationship);

        relationshipList.add(triplet);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return relationshipList;
  }

  @Override
  public Map<CachedNode, List<Pair<CachedNode, CachedRelationship>>> read2(String file) {
    Map<CachedNode, List<Pair<CachedNode, CachedRelationship>>> relationshipMap = new HashMap<CachedNode, List<Pair<CachedNode, CachedRelationship>>>();

    BufferedReader br = null;
    String line = null;
    CachedNode oldNode = new CachedNode(GraphReader.SENTRY_NODEID);
    String nA, nB, startNodeId, endNodeId, relationId;
    Node startNode = new Node(NodeType.SCHOLAR_PERSON, SCHOLAR_GRAPH_DUMMY_NAME);
    Node endNode = new Node(NodeType.SCHOLAR_PERSON, SCHOLAR_GRAPH_DUMMY_NAME);
    
    try {
      br = GzipFileReader.getFileReader(file);
      while ((line = br.readLine()) != null) {
        nA = StringUtil.GetBetween(line, "\"nA\": \"", "\",");
        nB = StringUtil.GetBetween(line, "\"nB\": \"", "\",");
        if (nA == null || nB == null) {
          System.out.println("Start Node: " + nA + "\tEnd Node: " + nB);
          continue;
        }
        
        startNode.setNameAndType(nA, NodeType.SCHOLAR_PERSON);
        endNode.setNameAndType(nB, NodeType.SCHOLAR_PERSON);
        
        // System.out.println("Start Node: " + startNode.getName() + "\nEnd Node: " + endNode.getName());
        // You have to call NodeIdGenerator.generate(Node node) first to get its node id 
        // before calling EdgeIdGenerator.generate(edge).
        startNodeId = Hex.encodeHex(NodeIdGenerator.generate(startNode));
        endNodeId = Hex.encodeHex(NodeIdGenerator.generate(endNode));
        
        Edge edge = new Edge(startNode, endNode);
        CachedNode start = new CachedNode(startNodeId);
        CachedNode end = new CachedNode(endNodeId);
        relationId = Hex.encodeHex(EdgeIdGenerator.generate(edge));
        CachedRelationship relationship = new CachedRelationship(relationId);

        if (!oldNode.getNodeId().equalsIgnoreCase(start.getNodeId())) {
          oldNode.setNodeId(start.getNodeId());
          Pair<CachedNode, CachedRelationship> pair = new Pair<CachedNode, CachedRelationship>(end,
              relationship);
          List<Pair<CachedNode, CachedRelationship>> rels = new ArrayList<Pair<CachedNode, CachedRelationship>>();
          rels.add(pair);
          relationshipMap.put(start, rels);
        } else {
          Pair<CachedNode, CachedRelationship> pair = new Pair<CachedNode, CachedRelationship>(end,
              relationship);
          List<Pair<CachedNode, CachedRelationship>> rels = relationshipMap.get(start);
          rels.add(pair);
          relationshipMap.put(start, rels);
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return relationshipMap;
  }

}
