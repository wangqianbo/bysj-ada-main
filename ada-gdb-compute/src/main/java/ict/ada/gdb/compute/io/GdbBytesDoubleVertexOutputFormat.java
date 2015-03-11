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

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>double</code> values and <code>float</code> out-edge weights
 */
public class GdbBytesDoubleVertexOutputFormat extends
  TextVertexOutputFormat<BytesWritable, DoubleWritable, FloatWritable> {

  @Override
  public TextVertexWriter createVertexWriter(
      TaskAttemptContext context) {
    return new GdbBytesDoubleVertexWriter();
  }

 /**
  * VertexWriter that supports vertices with <code>double</code>
  * values and <code>float</code> out-edge weights.
  */
  private class GdbBytesDoubleVertexWriter extends
    TextVertexWriterToEachLine {
	@Override
	protected Text convertVertexToLine(
			Vertex<BytesWritable, DoubleWritable, FloatWritable, ?> vertex)
			throws IOException {
		return new Text(StringUtils.byteToHexString(vertex.getId().getBytes())+'\t'+vertex.getValue());
	}
  }
}
