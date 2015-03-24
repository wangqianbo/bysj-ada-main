package ict.ada.gdb.compute.io;

import ict.ada.gdb.schema.NodeIdHTable;
import ict.ada.gdb.util.ValueDecoderUtil;

import java.io.IOException;
import java.util.Arrays;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.hbase.HBaseVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class GdbHBaseVertexOutputFormat extends HBaseVertexOutputFormat<BytesWritable,FloatFloatPairWritable,FloatWritable>{

	@Override
	public VertexWriter<BytesWritable, FloatFloatPairWritable, FloatWritable> createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		System.out.println(conf.get(TableOutputFormat.OUTPUT_TABLE));
		conf.set(TableOutputFormat.OUTPUT_TABLE, "gdb-nodeId-weibo-v1");
		return new GdbHBaseVertexWriter(context);
	}
private class GdbHBaseVertexWriter extends HBaseVertexWriter<BytesWritable,FloatFloatPairWritable,FloatWritable>{

	public GdbHBaseVertexWriter(TaskAttemptContext context) throws IOException,
			InterruptedException {
		super(context);
	}

	@Override
	public void writeVertex(
			Vertex<BytesWritable, FloatFloatPairWritable, FloatWritable> vertex)
			throws IOException, InterruptedException {
		BytesWritable vertexId = vertex.getId();
		byte[] id = Arrays.copyOf(vertexId.getBytes(), vertexId.getLength());
		byte[] value = ValueDecoderUtil.encode(vertex.getValue().getV1().get(),vertex.getValue().getV2().get());
		Put put = new Put(id);
		put.add(Bytes.toBytes(NodeIdHTable.FAMILY),Bytes.toBytes(GdbHBaseVertexInputFormat.COMPUTATION),value);
		getRecordWriter().write(new ImmutableBytesWritable() , put);
	}
	
}
}
