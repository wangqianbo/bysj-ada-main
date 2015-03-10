/**
 * 
 */
package ict.ada.graphcached.util;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

public class FileSystem {
  static public List<String> listDirectory(String directory) {
    List<String> files = new LinkedList<String>();
    
    LinkedList<File> list = new LinkedList<File>();
    File dir = new File(directory);
    File file[] = dir.listFiles();
    for (int i = 0; i < file.length; i++) {
      if (file[i].isDirectory())
        list.add(file[i]);
      else
        files.add(file[i].getAbsolutePath());
    }
    
    File tmp;
    while (!list.isEmpty()) {
      tmp = list.removeFirst();
      if (tmp.isDirectory()) {
        file = tmp.listFiles();
        if (file == null)
          continue;
        for (int i = 0; i < file.length; i++) {
          if (file[i].isDirectory())
            list.add(file[i]);
          else
            files.add(file[i].getAbsolutePath());
        }
      } else {
        files.add(tmp.getAbsolutePath());
      }
    }
    
    return files;
    
  }
}
