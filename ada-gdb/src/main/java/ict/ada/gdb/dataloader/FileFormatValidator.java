package ict.ada.gdb.dataloader;

import ict.ada.common.model.Edge;
import ict.ada.gdb.common.AdaConfig;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.dataloader.DataOperation.DataOpType;
import ict.ada.gdb.dataloader.GdbJsonDataProcessor.DataOpHandler;
import ict.ada.gdb.util.FileScanner;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

public class FileFormatValidator {
  private static final Log LOG = LogFactory.getLog(FileFormatValidator.class);

  private static void load(String filePath) throws Exception {
    GdbJsonDataProcessor processor = new GdbJsonDataProcessor(new DataOpHandler() {
      @Override
      public void handle(DataOperation dataOp) throws GdbException {
        if (!AdaConfig.GRAPH_ACCEPT_SELFLOOP && dataOp.getType() == DataOpType.ADD_EDGE) {
          Edge e = dataOp.getData();
          if (e.identicalHeadAndTail()) {
            throw new GdbException(
                "Edge has identical head and tail Nodes, which is forbidden by ada configuration");
          }
        }
      }

      @Override
      public void onFinish() throws GdbException {
      }
    });
    int counter = 0;
    FileScanner fs = new FileScanner(filePath);
    while (fs.hasNext()) {
      final int READ_BUF = 10 * 1024 * 1024;
      Path fp = fs.next();
      LOG.info("Starting to validate file: " + fp.toString());
      BufferedReader cin = new BufferedReader(new InputStreamReader(fs.getFileSystem().open(fp,
          READ_BUF), "UTF-8"), READ_BUF);
      try {
        String line;
        while (null != (line = cin.readLine())) {
          try {
            processor.process(line);
          } catch (GdbDataLoaderException e) {
            LOG.fatal("Validation failed. dataline=[" + line + "]", e);
            return;
          }
          catch (Exception e){
            LOG.fatal("Validation failed. dataline=[" + line + "]", e);
            return;
          }
          counter++;
          if (counter % 100000 == 0) LOG.info(counter + " lines processed");
        }
      } finally {
        cin.close();
      }
    }
    LOG.info(counter + " lines in total");
    LOG.info("Success.");
    LOG.info("NOTICE: Success here ONLY means no syntax errors in JSONs. The semantics of JSONs are not checked.");
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Please provide input file path.\n\nUSAGE:");
      System.err
          .println("If data is in HDFS, path should be like hdfs://10.0.99.18:8020/ada/json-data");
      System.err
          .println("If data is in local file system, path should be like file:///ada/json-data");
      System.err.println("All files in the given path will be processed recursively.");

      System.exit(-1);
    }
    try {
      load(args[0]);
    } catch (Exception e) {
      LOG.fatal("Exception in validation.", e);
    }
  }
}
