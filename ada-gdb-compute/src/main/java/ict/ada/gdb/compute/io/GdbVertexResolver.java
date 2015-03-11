package ict.ada.gdb.compute.io;

import java.util.List;

import ict.ada.common.model.RelationGraph;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.service.AdaGdbService;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.DefaultVertexResolver;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexChanges;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class GdbVertexResolver extends DefaultVertexResolver<BytesWritable, DoubleWritable, FloatWritable, DoubleWritable> {
	 public static final  AdaGdbService adaGdbService = new AdaGdbService(AdaModeConfig.GDBMode.QUERY);
	  private boolean createVertexesOnMessages = true;
//	  private ImmutableClassesGiraphConfiguration<BytesWritable, DoubleWritable, FloatWritable, DoubleWritable> conf = null;
//	  private GraphState<BytesWritable, DoubleWritable, FloatWritable, DoubleWritable> graphState;
	  /** Class logger */
	  private static final Logger LOG = Logger.getLogger(
			  GdbVertexResolver.class);
	
	
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
	  protected Vertex<BytesWritable, DoubleWritable, FloatWritable, DoubleWritable> addVertexIfDesired(
		 BytesWritable vertexId,
	      Vertex<BytesWritable, DoubleWritable, FloatWritable, DoubleWritable> vertex,
	      VertexChanges<BytesWritable, DoubleWritable, FloatWritable, DoubleWritable> vertexChanges,
	      boolean hasMessages) {
	    if (vertex == null) {
	      if (hasVertexAdditions(vertexChanges)) {
	        vertex = vertexChanges.getAddedVertexList().get(0);
	      } else if ((hasMessages && createVertexesOnMessages) ||
	                 hasEdgeAdditions(vertexChanges)) {
	        vertex = getConf().createVertex();
	        vertex.setGraphState(getGraphState());
	        ict.ada.common.model.Node  node = new ict.ada.common.model.Node(vertexId.getBytes());
			 RelQuerySpec.RelQuerySpecBuilder specBuilder = new RelQuerySpec.RelQuerySpecBuilder(node)
		        .attribute(node.getType().getAttribute());
			RelationGraph edgeList = null;
			try {
				edgeList = adaGdbService.queryRelationGraph(specBuilder.build());
			} catch (GdbException e) {
				throw new RuntimeException(e);
			}
			List<Edge<BytesWritable, FloatWritable>> edges =
			          Lists.newArrayListWithCapacity(edgeList.getOuterNodes().size());
			for(ict.ada.common.model.Node adj : edgeList.getOuterNodes()){
				edges.add(EdgeFactory.create(new BytesWritable(adj.getId()),new FloatWritable(0)));
			}
	        vertex.initialize(vertexId, getConf().createVertexValue(),edges);
	      }
	    } else if (hasVertexAdditions(vertexChanges)) {
	      LOG.warn("resolve: Tried to add a vertex with id = " +
	          vertex.getId() + " when one already " +
	          "exists.  Ignoring the add vertex request.");
	    }
	    return vertex;
	  }
}
