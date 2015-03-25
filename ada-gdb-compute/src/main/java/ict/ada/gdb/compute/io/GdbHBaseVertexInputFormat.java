package ict.ada.gdb.compute.io;

import ict.ada.gdb.service.AdaGdbService;
import ict.ada.gdb.util.ValueDecoderUtil;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.hbase.HBaseVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.javatuples.Pair;
public class GdbHBaseVertexInputFormat extends HBaseVertexInputFormat<BytesWritable,FloatFloatPairWritable,FloatWritable>{
	 /** Logger */
	  private static final Logger LOG =Logger.getLogger(GdbHBaseVertexInputFormat.class);
	public static final  AdaGdbService adaGdbService  = GdbVertexResolver.adaGdbService;
	public static final String COMPUTATION = "TR";
	public static final float INITIAL_VALUE = 1.0f;
	
	public static final float CHANGESIGN = 0.1f;
	@Override
	public VertexReader<BytesWritable, FloatFloatPairWritable, FloatWritable> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
				Configuration conf = context.getConfiguration();
				System.out.println(conf.get(TableInputFormat.INPUT_TABLE));
				conf.set(TableInputFormat.INPUT_TABLE, "gdb-nodeTask-weibo-v1");
//				conf.set(name, value);
				return new HBaseVertexReaderHandlingExceptions(split,context);
	}

	@Override
	public void checkInputSpecs(Configuration conf) {}

	public  class HBaseVertexReaderHandlingExceptions  extends HBaseVertexReader<BytesWritable,FloatFloatPairWritable,FloatWritable> {
		
		public HBaseVertexReaderHandlingExceptions(InputSplit split,
				TaskAttemptContext context) throws IOException {
			super(split, context);
		}
		@Override
	    public final boolean nextVertex() throws IOException, InterruptedException {
	      return getRecordReader().nextKeyValue();
	    }
		@SuppressWarnings("unchecked")
	    @Override
	    public final Vertex<BytesWritable,FloatFloatPairWritable,FloatWritable> getCurrentVertex() throws IOException, InterruptedException  {
	      Result result = getRecordReader().getCurrentValue();
	      List<KeyValue> values =  result.getColumn(Bytes.toBytes("i"),Bytes.toBytes(COMPUTATION));
	      float variation = 0.0f;
	      for(KeyValue value : values){
	    	  variation +=Bytes.toFloat(value.getValue());
	      }
	      Vertex<BytesWritable,FloatFloatPairWritable,FloatWritable> vertex ;
	      try {
	    	  byte[] value = adaGdbService.getNodeComputationValue(result.getRow(), COMPUTATION);
	    	  float v1 = 0.0f;
	    	  if(value == null){
	    		  v1 = INITIAL_VALUE;
	    	  }else{
	    		  Pair<Float,Float> decodeValue = ValueDecoderUtil.decode(value);
	    		  v1 = decodeValue.getValue0();
			      variation += decodeValue.getValue1();
	    	  }
	    	  FloatFloatPairWritable vertexValue = new FloatFloatPairWritable();
		      vertexValue.setV1(new FloatWritable(v1));
		      vertexValue.setV2(new FloatWritable(variation));
		      vertex = getConf().createVertex();
		      vertex.initialize(new BytesWritable(result.getRow()),vertexValue);
		      LOG.info("vertex's Value = ["+ vertex.getValue().getV1().get() + "," + vertex.getValue().getV2().get() +"]");
	      } catch (Throwable t) {
	        throw   new IllegalArgumentException(
			          "Couldn't get vertex " + StringUtils.byteToHexString(result.getRow()), t);
	      // CHECKSTYLE: resume IllegalCatch
	      }
	      return vertex;
	    }
	}

	
}
