package ict.ada.gdb.dataloader;

import java.io.File;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LocalJsonFileLoader {

  private static final Log LOG = LogFactory.getLog(LocalJsonFileLoader.class);

  private static void load(String filePath, int counterGap) throws Exception {
    if (counterGap <= 0) throw new IllegalArgumentException("counterGap=" + counterGap);

    GdbJsonDataProcessor processor = new GdbJsonDataProcessor();
    Scanner cin = new Scanner(new File(filePath), "UTF-8");
    int counter = 0;
    try {
      while (cin.hasNext()) {
        String line = cin.nextLine();
        processor.process(line);
        counter++;
        if (counter % counterGap == 0) LOG.info(counter + " lines processed");
      }
    } finally {
      try {
        processor.close();
      } catch (GdbDataLoaderException e) {
        e.printStackTrace();
      }
    }
    LOG.info(counter + " lines in total");
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      LOG.error("Please provide input file path, and optionally a counter gap");
      System.exit(-1);
    }
    if (args.length >= 2) {
      load(args[0], Integer.parseInt(args[1]));
    } else {
      load(args[0], 10000);
    }
  }

}
