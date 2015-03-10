package ict.ada.gdb.dataloader.mapred;

import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.AdaModeConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce Job for loading JSON file data into GDB.<br>
 * This is a Mapper-only job. Each line of JSON data file is read, parsed and written to GDB in
 * Mapper.
 * 
 * 
 */
public class JsonFileProcessJob extends Configured implements Tool {

  private static Log LOG = LogFactory.getLog(JsonFileProcessJob.class);

  @Override
  public int run(String[] arg0) throws Exception {
    if (arg0.length != 1) {
      LOG.fatal("need one argument: Input JSON file path");
      return -1;
    }
    String inputfolder = arg0[0];
    Job job = new Job(getConf(), "JSON File Process:" + inputfolder);

    job.setJarByClass(JsonFileProcessJob.class);
    job.setNumReduceTasks(0);// no Reducer
    job.setMapperClass(JsonFileProcessMapper.class);

    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPaths(job, inputfolder);
    job.setOutputFormatClass(NullOutputFormat.class);

    job.setSpeculativeExecution(false);// set to false because we write to HBase
    job.getConfiguration().set("mapred.map.max.attempts", "1");// never retry
    StringBuilder dbConf = new StringBuilder();
    StringBuilder indexConf = new StringBuilder();
    AdaModeConfig.setMode(AdaModeConfig.GDBMode.INSERT);
    for (Channel channel : Channel.values()) {
      if (!channel.equals(Channel.values()[0])) {
        dbConf.append(",");
        indexConf.append(",");
      }
      dbConf.append(channel.getIntForm() + ":" + AdaModeConfig.getDBVersion(channel));
      indexConf.append(channel.getIntForm() + ":" + AdaModeConfig.getIndexNumber(channel));
    }

    job.getConfiguration().set("gdb.db.insert.config", dbConf.toString());
    job.getConfiguration().set("gdb.db.index.config", indexConf.toString());
    job.waitForCompletion(true);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new JsonFileProcessJob(), args);
  }

}
