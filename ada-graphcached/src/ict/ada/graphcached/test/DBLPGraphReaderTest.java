/**
 * 
 */
package ict.ada.graphcached.test;

import java.util.List;

import ict.ada.graphcached.core.GraphCachedImpl;
import ict.ada.graphcached.model.CachedNode;
import ict.ada.graphcached.model.CachedRelationship;
import ict.ada.graphcached.process.DBLPGraphReader;
import ict.ada.graphcached.util.Triplet;

/**
 * @author forhappy
 *
 */
public class DBLPGraphReaderTest {

  /**
   * @param args
   */
  public static void main(String[] args) {
    GraphCachedImpl graphCached = new GraphCachedImpl();
    
    DBLPGraphReader dr = new DBLPGraphReader();
    List<Triplet<CachedNode, CachedNode, CachedRelationship>> relationshipList = 
        dr.read("raw/com-dblp.ungraph.txt.gz");
    
    System.out.println("Read relationships: " + relationshipList.size());
    
    long startTime = System.currentTimeMillis();
    System.out.println("Begin importing: " + startTime + "ms");
    
    graphCached.addRelationships(relationshipList);
    
    long endTime = System.currentTimeMillis();
    System.out.println("End importing: " + endTime + "ms");
    
    System.out.println("Time elapsed: " + (endTime - startTime) + "ms");
  }

}
