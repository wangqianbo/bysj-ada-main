/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ict.ada.gdb.compute.io;

import ict.ada.common.model.RelationGraph;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.common.RelQuerySpec;
import ict.ada.gdb.service.AdaGdbService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.giraph.Algorithm;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
@Algorithm(
    name = "Trust rank"
)
public class WeiboTrustRankComputation extends BasicComputation<BytesWritable,FloatFloatPairWritable, FloatWritable, FloatWritable> {
  /** Number of supersteps for this test */
  public static final int MAX_SUPERSTEPS = 30;
  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(WeiboTrustRankComputation.class);
  /** Sum aggregator name */
  private static String SUM_AGG = "sum";
  /** Min aggregator name */
  private static String MIN_AGG = "min";
  /** Max aggregator name */
  private static String MAX_AGG = "max";
  public static final  AdaGdbService adaGdbService  = GdbVertexResolver.adaGdbService;

  @Override
  public void compute(
      Vertex<BytesWritable, FloatFloatPairWritable, FloatWritable> vertex,
      Iterable<FloatWritable> messages) throws IOException {
    if (getSuperstep() >= 1) {
      float sum = 0;
      for (FloatWritable message : messages) {
        sum += message.get();
      }
      FloatFloatPairWritable vertexValue = vertex.getValue();
      vertexValue.getV1().set(vertexValue.getV2().get()+sum);
//      Math.abs(paramDouble)
      vertex.setValue(vertexValue);
      FloatWritable vertexCurValue = new FloatWritable(vertexValue.getV1().get() + vertexValue.getV2().get());
      aggregate(MAX_AGG,vertexCurValue );
      aggregate(MIN_AGG, vertexCurValue);
      aggregate(SUM_AGG, new LongWritable(1));
      LOG.info(StringUtils.byteToHexString(Bytes.head(vertex.getId().getBytes(),vertex.getId().getLength())) + ": TrustRank=" +  vertexCurValue.get()
    		  + " change = "+vertex.getValue().getV2().get());
    }
    FloatFloatPairWritable vertexValue = vertex.getValue();
    if (getSuperstep() < MAX_SUPERSTEPS && Math.abs(vertexValue.getV2().get()) >= GdbHBaseVertexInputFormat.CHANGESIGN) { // 此处可以提取出来，作为一个接口
      loadEdgesIfNeed(vertex);
      int edges = vertex.getNumEdges();
      LOG.info("Vertex Edges num = " + edges);
      float change = vertexValue.getV2().get();
      if(edges != 0){
          sendMessageToAllEdges(vertex,new FloatWritable(change/ edges));
      }
      vertexValue.getV1().set(vertexValue.getV1().get() + change);
      vertexValue.getV2().set(0.0f);
    } else {
      vertex.voteToHalt();
      }
    }
    private void loadEdgesIfNeed(Vertex<BytesWritable,FloatFloatPairWritable, FloatWritable> vertex){
    	if(vertex.getNumEdges() !=0) return;
    	BytesWritable vertexId = vertex.getId();
    	byte[] id = Arrays.copyOf(vertexId.getBytes(), vertexId.getLength());
		LOG.info("Add Node Id = " + StringUtils.byteToHexString(id));
        ict.ada.common.model.Node  node = new ict.ada.common.model.Node(id);
		 RelQuerySpec.RelQuerySpecBuilder specBuilder = new RelQuerySpec.RelQuerySpecBuilder(node).attribute(node.getType().getAttribute());
		RelationGraph edgeList = null;
		try {
			edgeList = adaGdbService.queryRelationGraph(specBuilder.build());
			LOG.info("Result Size  = " + edgeList.getOuterNodes().size());
		} catch (GdbException e) {
			throw new RuntimeException(e);
		}
		List<Edge<BytesWritable, FloatWritable>> edges =
		          Lists.newArrayListWithCapacity(edgeList.getOuterNodes().size());
		for(ict.ada.common.model.Node adj : edgeList.getOuterNodes()){
			edges.add(EdgeFactory.create(new BytesWritable(adj.getId()),new FloatWritable(0)));
		}
		vertex.setEdges(edges);
    }
  /**
   * Worker context used with {@link WeiboTrustRankComputation}.
   */
  public static class SimpleTrustRankWorkerContext extends
      WorkerContext {
    /** Final max value for verification for local jobs */
    private static float FINAL_MAX;
    /** Final min value for verification for local jobs */
    private static float FINAL_MIN;
    /** Final sum value for verification for local jobs */
    private static long FINAL_SUM;

    public static double getFinalMax() {
      return FINAL_MAX;
    }

    public static double getFinalMin() {
      return FINAL_MIN;
    }

    public static long getFinalSum() {
      return FINAL_SUM;
    }

    @Override
    public void preApplication()
      throws InstantiationException, IllegalAccessException {
    }

    @Override
    public void postApplication() {
      FINAL_SUM = this.<LongWritable>getAggregatedValue(SUM_AGG).get();
      FINAL_MAX = this.<FloatWritable>getAggregatedValue(MAX_AGG).get();
      FINAL_MIN = this.<FloatWritable>getAggregatedValue(MIN_AGG).get();

      LOG.info("aggregatedNumVertices=" + FINAL_SUM);
      LOG.info("aggregatedMaxPageRank=" + FINAL_MAX);
      LOG.info("aggregatedMinPageRank=" + FINAL_MIN);
    }

    @Override
    public void preSuperstep() {
      if (getSuperstep() >= 3) {
        LOG.info("aggregatedNumVertices=" +
            getAggregatedValue(SUM_AGG) +
            " NumVertices=" + getTotalNumVertices());
        if (this.<LongWritable>getAggregatedValue(SUM_AGG).get() !=
            getTotalNumVertices()) {
          throw new RuntimeException("wrong value of SumAggreg: " +
              getAggregatedValue(SUM_AGG) + ", should be: " +
              getTotalNumVertices());
        }
        FloatWritable maxTrustrank = getAggregatedValue(MAX_AGG);
        LOG.info("aggregatedMaxPageRank=" + maxTrustrank.get());
        FloatWritable minTrustrank = getAggregatedValue(MIN_AGG);
        LOG.info("aggregatedMinPageRank=" + minTrustrank.get());
      }
    }

    @Override
    public void postSuperstep() { }
  }
}
