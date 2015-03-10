/**
 * 
 */
package ict.ada.graphcached.test;

import java.util.Random;

import org.neo4j.graphdb.Path;

import ict.ada.graphcached.core.GraphCachedImpl;
import ict.ada.graphcached.model.CachedNode;

/**
 * @author forhappy
 *
 */
public class PathQueryTest {
  
  final static int MAX_PATH_QUERY_TRIES = 10000;
  final static int MAX_NODEID = 3000000;

  /**
   * @param args
   */
  public static void main(String[] args) {
    GraphCachedImpl graphCached = new GraphCachedImpl();
    long totalTime = 0;
    Random r = new Random();
    for (int i = 0; i < MAX_PATH_QUERY_TRIES; i++) {
      int startId = r.nextInt(MAX_NODEID);
      int endId = r.nextInt(MAX_NODEID);
      if (startId == endId) continue;
      CachedNode startNode = new CachedNode(String.valueOf(startId));
      CachedNode endNode = new CachedNode(String.valueOf(endId));
      
//      long startTime = System.currentTimeMillis();
//      Iterable<Path> paths = graphCached.allPaths(startNode, endNode);
//      long endTime = System.currentTimeMillis();
//      System.out.println("" + (endTime - startTime) + "");
 
//      long startTime = System.currentTimeMillis();
//      Iterable<Path> paths = graphCached.allSimplePaths(startNode, endNode);
//      long endTime = System.currentTimeMillis();
//      System.out.println("" + (endTime - startTime) + "");
      long startTime = System.currentTimeMillis();
      Iterable<Path> paths = graphCached.findShortestPath(startNode, endNode);
      long endTime = System.currentTimeMillis();
      System.out.println("" + (endTime - startTime) + "");
      totalTime += endTime - startTime;
      
//      if (paths == null) continue;
//      for (Path path: paths) {
//        System.out.println(path.toString());
//      }
      
    }
    
    System.out.println("Total Time elapsed: " + totalTime + "ms");
  }

}
