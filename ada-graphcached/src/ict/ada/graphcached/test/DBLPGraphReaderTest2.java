/**
 * 
 */
package ict.ada.graphcached.test;

import java.util.List;
import java.util.Map;

import ict.ada.graphcached.core.GraphCachedImpl;
import ict.ada.graphcached.model.CachedNode;
import ict.ada.graphcached.model.CachedRelationship;
import ict.ada.graphcached.process.DBLPGraphReader;
import ict.ada.graphcached.util.Pair;

/**
 * @author forhappy
 *
 */
public class DBLPGraphReaderTest2 {

  /**
   * @param args
   */
  public static void main(String[] args) {
    GraphCachedImpl graphCached = new GraphCachedImpl();
    
    DBLPGraphReader dr = new DBLPGraphReader();
    Map<CachedNode, List<Pair<CachedNode, CachedRelationship>>> relationshipMap = 
        dr.read2("raw/com-dblp.ungraph.txt.gz");
    
    System.out.println("Read start nodes: " + relationshipMap.size());
    
    long startTime = System.currentTimeMillis();
    System.out.println("Begin importing: " + startTime + "ms");
    
    graphCached.addRelationships(relationshipMap);
    
    long endTime = System.currentTimeMillis();
    System.out.println("End importing: " + endTime + "ms");
    
    System.out.println("Time elapsed: " + (endTime - startTime) + "ms");
  }

}
