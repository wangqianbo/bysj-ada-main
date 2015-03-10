package ict.ada.gdb.cache.mapred;

import ict.ada.common.model.NodeType.Channel;
import ict.ada.common.util.Timer;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.AdaModeConfig.GDBMode;
import ict.ada.gdb.schema.GdbHTablePartitionPolicy;
import ict.ada.gdb.schema.GdbHTableType;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * MapReduce Job to collect statistics for graphs.
 * Read HBase tables, and calculate each Nodes' out-degree, memory cost in GdbCache, distinct
 * relation-type count, distinct node-type count, etc.
 * 
 * Output is written in an HDFS text file, one line for one node. Default output dir is
 * /gdbcache/graph_stat/CHANNEL-NAME
 * 
 * Note: because we partition each Node's relations into multiple HBase Region, we HAVE TO use
 * reducers to aggregate all data for one Node and then calculate "distinct" relation-type count,
 * etc.
 * 
 */
public class GraphStatisticJob extends Configured implements Tool {

  private Channel channel;

  private void run() throws IOException, InterruptedException, ClassNotFoundException {

    String jobName = "GraphStatisticJob-" + channel + "-" + (System.currentTimeMillis() / 1000);
    Job job = new Job(getConf(), jobName);
    job.setJarByClass(GraphStatisticJob.class);

    Scan scan = new Scan();
    scan.setCaching(5000); // 1 is the default in Scan, which will be bad for MapReduce jobs
    scan.setCacheBlocks(false); // don't set to true for MR jobs

    String htableName = GdbHTablePartitionPolicy.getEdgeRelatedHTableNameWithoutAggType(
        GdbHTableType.EDGE_REL_WEIGHT_SUM, channel);
    System.out.println("HBase Table Name: " + htableName);

    TableMapReduceUtil.initTableMapperJob(htableName, scan, GraphStatisticMapper.class,
        ImmutableBytesWritable.class, Text.class, job);

    job.setSpeculativeExecution(false);// set to false because we write to HBase
    job.getConfiguration().set("mapred.map.max.attempts", "1");// never retry

    job.getConfiguration().set("mapred.compress.map.output", "true");// enable Mapper output
                                                                     // compression

    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job, new Path("/gdbcache/graph_stat/" + channel));
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setReducerClass(GraphStatisticReducer.class);

    job.setReduceSpeculativeExecution(false);
    job.setNumReduceTasks(8);// reducer number

    System.out.println("Start to submit job...");

    job.waitForCompletion(true);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: command ChannelNumber");
      return -1;
    }
    this.channel = Channel.getChannel(Integer.parseInt(args[0]));
    if (channel == null) {
      System.err.println("Bad Channel number:" + args[0]);
      return -1;
    } else {
      System.out.println("Starting MapReduce job to collect statistics for Channel " + channel
          + " ...");
    }
    long start = Timer.now();
    run();
    System.out.println("MapReduce job collects statistics for Channel " + channel + " in "
        + Timer.msSince(start) + "ms");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration config = HBaseConfiguration.create();
    AdaModeConfig.setMode(GDBMode.QUERY);// !!!
    ToolRunner.run(config, new GraphStatisticJob(), args);
  }

}
