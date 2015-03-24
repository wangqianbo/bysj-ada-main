package ict.ada.gdb.compute.io;

import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.service.AdaGdbService;
import ict.ada.gdb.util.ValueDecoderUtil;

import java.util.Arrays;

import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexChanges;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

public class GdbHBaseVertexResolver extends DefaultVertexResolver<BytesWritable,FloatFloatPairWritable, FloatWritable> {
	 public static final  AdaGdbService adaGdbService = new AdaGdbService(AdaModeConfig.GDBMode.QUERY);
	  private boolean createVertexesOnMessages = true;
//	  private ImmutableClassesGiraphConfiguration<BytesWritable, DoubleWritable, FloatWritable, DoubleWritable> conf = null;
//	  private GraphState<BytesWritable, DoubleWritable, FloatWritable, DoubleWritable> graphState;
	  /** Class logger */
	  private static final Logger LOG = Logger.getLogger(
			  GdbHBaseVertexResolver.class);
	
	
	/**
	   * Add the Vertex if desired. Returns the vertex itself, or null if no vertex
	   * added.
	   *
	   * @param vertexId ID of vertex
	   * @param vertex Vertex, if not null just returns it as vertex already exists
	   * @param vertexChanges specifies if we should add the vertex
	   * @param hasMessages true if this vertex received any messages
	   * @return Vertex created or passed in, or null if no vertex should be added
	   */
	@Override
	  protected Vertex<BytesWritable,FloatFloatPairWritable, FloatWritable> addVertexIfDesired(
		 BytesWritable vertexId,
	      Vertex<BytesWritable,FloatFloatPairWritable, FloatWritable> vertex,
	      VertexChanges<BytesWritable,FloatFloatPairWritable, FloatWritable> vertexChanges,
	      boolean hasMessages) {
	    if (vertex == null) {
	        if (hasVertexAdditions(vertexChanges)) {
	          vertex = vertexChanges.getAddedVertexList().get(0);
	        } else if ((hasMessages && createVertexesOnMessages) ||
	                   hasEdgeAdditions(vertexChanges)) {
	        		vertex = getConf().createVertex();
	        		
//	       	        vertex.setGraphState(getGraphState());
	        		byte[] id = Arrays.copyOf(vertexId.getBytes(), vertexId.getLength());
	        		byte[] value = null;
					try {
						value = adaGdbService.getNodeComputationValue(id, GdbHBaseVertexInputFormat.COMPUTATION);
						if(value == null){
							FloatFloatPairWritable vertexValue = new FloatFloatPairWritable();
							vertexValue.setV1(new FloatWritable(0.0f));
							vertexValue.setV2(new FloatWritable(1.0f));
							vertex.initialize(vertexId,vertexValue);
						}else{
							Pair<Float, Float> valuePair = ValueDecoderUtil.decode(value);
							FloatFloatPairWritable vertexValue = new FloatFloatPairWritable();
							vertexValue.setV1(new FloatWritable(valuePair.getValue0()));
							vertexValue.setV2(new FloatWritable(valuePair.getValue1()));
							vertex.initialize(vertexId,vertexValue);
						}
						
//		        		LOG.info("Add Node Id = " + StringUtils.byteToHexString(id));
					} catch (GdbException e) {
						LOG.error("", e);
					}
	    	        }
	      } else if (hasVertexAdditions(vertexChanges)) {
	        LOG.warn("resolve: Tried to add a vertex with id = " +
	            vertex.getId() + " when one already " +
	            "exists.  Ignoring the add vertex request.");
	      }
	      return vertex;
	    }
}
