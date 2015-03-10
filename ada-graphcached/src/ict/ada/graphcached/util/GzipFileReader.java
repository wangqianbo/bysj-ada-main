/**
 * 
 */
package ict.ada.graphcached.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

/**
 * @author forhappy
 * 
 */
public class GzipFileReader {

  static public BufferedReader getFileReader(final String file)
      throws IOException, FileNotFoundException {
    BufferedReader fileReader;
    // support compressed files
    if (file.endsWith(".gz")) {
      fileReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(
          new FileInputStream(file))));
    } else if (file.endsWith(".zip")) {
      fileReader = new BufferedReader(new InputStreamReader(new ZipInputStream(new FileInputStream(
          file))));
    } else {
      fileReader = new BufferedReader(new FileReader(file));
    }

    return fileReader;
  }

}
