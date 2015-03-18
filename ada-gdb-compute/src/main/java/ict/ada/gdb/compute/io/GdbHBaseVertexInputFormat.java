package ict.ada.gdb.compute.io;

import ict.ada.gdb.service.AdaGdbService;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.hbase.HBaseVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.javatuples.Pair;
public class GdbHBaseVertexInputFormat extends HBaseVertexInputFormat<BytesWritable,PairWritable<FloatWritable,FloatWritable>,FloatWritable>{
	public static final  AdaGdbService adaGdbService  = GdbVertexResolver.adaGdbService;
	public static final String COMPUTATION = "TR";
	public static final float INITIAL_VALUE = 1.0f;
	
	public static final float CHANGESIGN = 1.0f;
	@Override
	public VertexReader<BytesWritable, PairWritable<FloatWritable, FloatWritable>, FloatWritable> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
				return new HBaseVertexReaderHandlingExceptions(split,context);
	}

	@Override
	public void checkInputSpecs(Configuration conf) {}

	public  class HBaseVertexReaderHandlingExceptions  extends HBaseVertexReader<BytesWritable,PairWritable<FloatWritable,FloatWritable>,FloatWritable> {
		
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
	    public final Vertex<BytesWritable,PairWritable<FloatWritable,FloatWritable>,FloatWritable> getCurrentVertex() throws IOException, InterruptedException  {
	      Result result = getRecordReader().getCurrentValue();
	      List<KeyValue> values =  result.getColumn(Bytes.toBytes("i"),Bytes.toBytes(COMPUTATION));
	      float variation = 0.0f;
	      for(KeyValue value : values){
	    	  variation +=Bytes.toFloat(value.getValue());
	      }
	      Vertex<BytesWritable,PairWritable<FloatWritable,FloatWritable>,FloatWritable> vertex ;
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
		      PairWritable<FloatWritable,FloatWritable> vertexValue = new PairWritable<FloatWritable,FloatWritable>();
		      vertexValue.setValue1(new FloatWritable(v1));
		      vertexValue.setValue2(new FloatWritable(variation));
		      vertex = getConf().createVertex();
		      vertex.initialize(new BytesWritable(result.getRow()),vertexValue);
	      } catch (Throwable t) {
	        throw   new IllegalArgumentException(
			          "Couldn't get vertex " + StringUtils.byteToHexString(result.getRow()), t);
	      // CHECKSTYLE: resume IllegalCatch
	      }
	      return vertex;
	    }
	}

	
}
