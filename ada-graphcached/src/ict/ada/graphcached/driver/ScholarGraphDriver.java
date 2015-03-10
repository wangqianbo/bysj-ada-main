/**
 * 
 */
package ict.ada.graphcached.driver;

import java.util.List;
import java.util.Map;

import ict.ada.graphcached.core.GraphCachedImpl;
import ict.ada.graphcached.model.CachedNode;
import ict.ada.graphcached.model.CachedRelationship;
import ict.ada.graphcached.process.ScholarGraphReader;
import ict.ada.graphcached.util.FileSystem;
import ict.ada.graphcached.util.Pair;
import ict.ada.graphcached.util.Triplet;

public class ScholarGraphDriver {
  static GraphCachedImpl graphCached = new GraphCachedImpl();
  static ScholarGraphReader gr = new ScholarGraphReader();
  
  private static void processSingleFile(String file) {
    List<Triplet<CachedNode, CachedNode, CachedRelationship>> relationshipList = gr.read(file);
    System.out.println("Importing #relations: " + relationshipList.size());
    graphCached.addRelationships(relationshipList);
  }
  
  private static void processSingleFile2(String file) {
    Map<CachedNode, List<Pair<CachedNode, CachedRelationship>>> relationshipMap = gr.read2(file);
    graphCached.addRelationships(relationshipMap);
  }
  
  private static void processDirectory(String dir) {
    List<String> files = FileSystem.listDirectory(dir);
    
    for (String file: files) {
      System.out.println("Beginning processing file: " + file);
      processSingleFile(file);
      System.out.println("Finishing processing file: " + file);
    }
    
    System.out.println(files.size() + " files processed.");
    
  }
  
  private static void processDirectory2(String dir) {
    List<String> files = FileSystem.listDirectory(dir);
    
    for (String file: files) {
      System.out.println("Beginning processing file: " + file);
      processSingleFile2(file);
      System.out.println("Finishing processing file: " + file);
    }
    
    System.out.println(files.size() + " files processed.");
    
  }
  
  private static void Usage() {
    System.out.println("Usage: ScholarGraphDriver dir");
  }
  
  public static void main(String args[]) {
    if (args.length != 1) {
      Usage();
      return;
    }
    System.out.println("Processing " + args[0]);
    processDirectory(args[0]);
    
  }
  
  
}
