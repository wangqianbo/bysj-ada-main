/**
 * 
 */
package ict.ada.graphcached.test;

import ict.ada.graphcached.util.FileSystem;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

public class FileSystemTest {
  public static void main(String[] args) {
    
    List<String> files = FileSystem.listDirectory("lib");
    for (String file: files) {
      System.out.println(file);
    }
    
  }
}