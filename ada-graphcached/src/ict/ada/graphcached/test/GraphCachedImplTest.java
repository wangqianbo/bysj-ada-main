/**
 * 
 */
package ict.ada.graphcached.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import ict.ada.graphcached.core.GraphCachedImpl;
import ict.ada.graphcached.model.CachedNode;
import ict.ada.graphcached.model.CachedRelationship;
import ict.ada.graphcached.util.Triplet;

/**
 * @author forhappy
 * 
 */
public class GraphCachedImplTest {

  private static int RELATIONSHIPS = 100000;

  /**
   * @param args
   */
  public static void main(String[] args) {
    GraphCachedImpl graphCached = new GraphCachedImpl();
    
    List<Triplet<CachedNode, CachedNode, CachedRelationship>> relationshipList =
        new ArrayList<Triplet<CachedNode,CachedNode,CachedRelationship>>();
    Random r = new Random();
    for (int i = 0; i < RELATIONSHIPS; i++) {
      int seed = r.nextInt(RELATIONSHIPS);
      int offset = r.nextInt(RELATIONSHIPS);
      
      CachedNode start = new CachedNode(String.valueOf(seed));
      CachedNode end = new CachedNode(String.valueOf(seed + 1));
      CachedRelationship relationship = new CachedRelationship(String.valueOf(seed + offset + 100000));
      
      Triplet<CachedNode,CachedNode,CachedRelationship> triplet =
          new Triplet<CachedNode, CachedNode, CachedRelationship>(start, end, relationship);
      relationshipList.add(triplet);
    }
    
    long startTime = System.currentTimeMillis();
    System.out.println("Start time: " + startTime + "ms");
    
    graphCached.addRelationships(relationshipList);
    
    long endTime = System.currentTimeMillis();
    System.out.println("End time: " + endTime + "ms");
    
    System.out.println("Time elapsed: " + (endTime - startTime) + "ms");
  }
}
