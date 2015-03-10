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

import ict.ada.graphcached.core.GraphReader;
import ict.ada.graphcached.model.CachedNode;
import ict.ada.graphcached.model.CachedRelationship;
import ict.ada.graphcached.util.GzipFileReader;
import ict.ada.graphcached.util.Pair;
import ict.ada.graphcached.util.Triplet;

/**
 * @author forhappy
 *
 */
public class DBLPGraphReader implements GraphReader {

  /* (non-Javadoc)
   * @see ict.ada.graphcached.core.GraphReader#read(java.lang.String)
   */
  @Override
  public List<Triplet<CachedNode, CachedNode, CachedRelationship>> read(String file) {
    List<Triplet<CachedNode, CachedNode, CachedRelationship>> relationshipList =
        new ArrayList<Triplet<CachedNode,CachedNode,CachedRelationship>>();
    BufferedReader br = null;
    String line = null;
    int lineCount = 0;
    try {
      br = GzipFileReader.getFileReader(file);
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#")) continue;
        String[] nodes = line.split("\t");
        CachedNode start = new CachedNode(nodes[0]);
        CachedNode end = new CachedNode(nodes[1]);
        CachedRelationship relationship = new CachedRelationship(String.valueOf(lineCount++));
        
        Triplet<CachedNode,CachedNode,CachedRelationship> triplet =
            new Triplet<CachedNode, CachedNode, CachedRelationship>(start, end, relationship);
        
        relationshipList.add(triplet);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    return relationshipList;
  }

  /* (non-Javadoc)
   * @see ict.ada.graphcached.core.GraphReader#read2(java.lang.String)
   */
  @Override
  public Map<CachedNode, List<Pair<CachedNode, CachedRelationship>>> read2(String file) {
    Map<CachedNode, List<Pair<CachedNode, CachedRelationship>>> relationshipMap =
        new HashMap<CachedNode, List<Pair<CachedNode,CachedRelationship>>>();
    
    BufferedReader br = null;
    String line = null;
    int lineCount = 0;
    CachedNode oldNode = new CachedNode(GraphReader.SENTRY_NODEID);
    
    try {
      br = GzipFileReader.getFileReader(file);
      while ((line = br.readLine()) != null) {
        if (line.startsWith("#")) continue;
        String[] nodes = line.split("\t");
        
        CachedNode start = new CachedNode(nodes[0]);
        CachedNode end = new CachedNode(nodes[1]);
        CachedRelationship relationship = new CachedRelationship(String.valueOf(lineCount++));
        
        if (!oldNode.getNodeId().equalsIgnoreCase(start.getNodeId())) {
          oldNode.setNodeId(start.getNodeId());
          Pair<CachedNode,CachedRelationship> pair =
              new Pair<CachedNode, CachedRelationship>(end, relationship);
          List<Pair<CachedNode,CachedRelationship>> rels = 
              new ArrayList<Pair<CachedNode,CachedRelationship>>();
          rels.add(pair);
          relationshipMap.put(start, rels);
        } else {
          Pair<CachedNode,CachedRelationship> pair =
              new Pair<CachedNode, CachedRelationship>(end, relationship);
          List<Pair<CachedNode,CachedRelationship>> rels = relationshipMap.get(start);
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
