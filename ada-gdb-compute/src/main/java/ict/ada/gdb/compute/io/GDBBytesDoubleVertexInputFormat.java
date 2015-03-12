package ict.ada.gdb.compute.io;

import ict.ada.common.model.RelationGraph;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.service.AdaGdbService;

import java.io.IOException;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.javatuples.Pair;

import com.google.common.collect.Lists;

/**
 * GDBBytesDoubleVertexInputFormat that features <code>bytes</code> vertex ID's,
 * <code>double</code> vertex values and <code>float</code> out-edge weights,
 * and <code>double</code> message types, specified in JSON format.
 */
public class GDBBytesDoubleVertexInputFormat extends
		TextVertexInputFormat<BytesWritable, DoubleWritable, FloatWritable> {
	 private static final Logger LOG = Logger.getLogger(GDBBytesDoubleVertexInputFormat.class);
	@Override
	public TextVertexReader createVertexReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		
		return new GDBBytesDoubleVertexReader();
	}

	/**
	 * VertexReader that features <code>double</code> vertex values and
	 * <code>float</code> out-edge weights.
	 */
	  class GDBBytesDoubleVertexReader  extends
    TextVertexReaderFromEachLineProcessedHandlingExceptions<Pair<byte[],Double>,GdbException>{
		  private AdaGdbService adaGdbService = GdbVertexResolver.adaGdbService;

		@Override
		protected Pair<byte[],Double> preprocessLine(Text line) throws GdbException,
				IOException {
				String lineStr = line.toString();
				if(lineStr == null || lineStr.isEmpty())
					throw new GdbException("Empty line!");
				String[] lineSplit = lineStr.split(",");
				if(lineSplit[0].length() != ict.ada.common.model.Node.NODEID_SIZE*2)
					throw new GdbException("illegal Node id!");
				return new Pair<byte[],Double>(StringUtils.hexStringToByte(lineSplit[0]), Double.parseDouble(lineSplit[1]));
		}

		 @Override
		    protected BytesWritable getId(Pair<byte[],Double> vertex) throws GdbException,
		              IOException {
		      return new BytesWritable(vertex.getValue0());
		    }

		@Override
		protected DoubleWritable getValue(Pair<byte[],Double> vertex) throws GdbException,
				IOException {
			return new DoubleWritable(vertex.getValue1());
		}

		@Override
		protected Iterable<Edge<BytesWritable, FloatWritable>> getEdges(
				Pair<byte[],Double> vertex) throws GdbException, IOException {
			ict.ada.common.model.Node  node = new ict.ada.common.model.Node(vertex.getValue0());
			LOG.info("Node Id = "+ StringUtils.byteToHexString(node.getId()));
			 RelQuerySpec.RelQuerySpecBuilder specBuilder = new RelQuerySpec.RelQuerySpecBuilder(node)
		        .attribute(node.getType().getAttribute());
			RelationGraph edgeList = adaGdbService.queryRelationGraph(specBuilder.build());
			LOG.info("EdgeList size = "+ edgeList.getOuterNodes().size());
			List<Edge<BytesWritable, FloatWritable>> edges =
			          Lists.newArrayListWithCapacity(edgeList.getOuterNodes().size());
			for(ict.ada.common.model.Node adj : edgeList.getOuterNodes()){
				edges.add(EdgeFactory.create(new BytesWritable(adj.getId()),new FloatWritable(0)));
			}
			return edges;
		}
		 @Override
		    protected Vertex<BytesWritable, DoubleWritable, FloatWritable> handleException(Text line, Pair<byte[],Double> vertex,
		            		  GdbException e) {
		      throw new IllegalArgumentException(
		          "Couldn't get vertex from line " + line, e);
		    }
	}
}
