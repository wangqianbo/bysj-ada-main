package ict.ada.gdb.dataloader.mapred;

import ict.ada.common.util.Timer;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.dataloader.GdbDataLoaderException;
import ict.ada.gdb.dataloader.GdbJsonDataProcessor;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JsonFileProcessMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
  /*
   * Counters
   */
  private static final String GROUP = "GDB JSON Process";
  private static final String CNT_SUCCESS_LINE = "Success JSON Lines";
  private static final String CNT_EXCEPTIONS = "Exceptions";

  /*
   * JSON Data processor
   */
  private GdbJsonDataProcessor processor;
  
  private long jsonLines = 0;
  private long lastReportedLines=0;

  private long exceptionCounter = 0;
  private static final int MAX_EXCEPTIONS = 1;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    processor = new GdbJsonDataProcessor();
    exceptionCounter = 0;
    context.setStatus(context.getInputSplit().toString());
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
    String line = value.toString();
    try {
      processor.process(line);
      jsonLines++;
      if (jsonLines % 1024 == 0) {
        context.getCounter(GROUP, CNT_SUCCESS_LINE).increment(jsonLines - lastReportedLines);
        lastReportedLines = jsonLines;
      }
    } catch (GdbDataLoaderException e) { 
      e.printStackTrace();
      context.getCounter(GROUP, CNT_EXCEPTIONS).increment(1);
      exceptionCounter++;
      // Due to the search index write buffer in HBaseNodeDAO, a exception here may in fact affect
      // multiple lines!
      if (exceptionCounter >= MAX_EXCEPTIONS) {// exit the job if too many exceptions
        throw new RuntimeException("Too many exceptions.", e);
      }
    }
  }

  @Override
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    AdaModeConfig.loadConfig(context.getConfiguration().get("gdb.db.insert.config"),context.getConfiguration().get("gdb.db.index.config"));
    long startTs=Timer.now();
    try {
      while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
    } finally {// make sure processor is always closed so that internal cache is flushed.
      double t = Timer.msSince(startTs);
      System.out.println("Mapper Throughput: " + ((jsonLines * 1000) / t) + " lines/second");
      context.getCounter(GROUP, CNT_SUCCESS_LINE).increment(jsonLines - lastReportedLines);
      if (processor != null) {
        try {
          processor.close();
        } catch (GdbDataLoaderException e) {
          // do not throw exception in finally block
          e.printStackTrace();
          // exception here may hide exception from map()..
          throw new RuntimeException("exception when closing JsonFileProcessor.",e);
        }
      }
    }
    cleanup(context);
  }

}
