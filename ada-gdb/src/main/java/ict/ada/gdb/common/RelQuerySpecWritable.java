package ict.ada.gdb.common;

import ict.ada.common.model.Node;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.RelationType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.WdeRef;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RelQuerySpecWritable implements Writable {
  private BytesWritable centerNode;// center node for the RelationGraph
  
 
  private MapWritable requiredRelType;// required Relation type on Edge
  private IntWritable requiredAttribute;// required Node type on the other end of Edge
  private LongWritable startSecInclusive;// in seconds
  private LongWritable endSecExclusive;// in seconds
  private IntWritable resultSize;// Edge count in the result RelationGraph
  private BooleanWritable useRelRank;// whether to get Top-n Edges according to weights.
  private MapWritable containedWdeIds;
  public RelQuerySpecWritable() {
    centerNode = new BytesWritable();
    
    requiredRelType = new MapWritable();
    requiredAttribute = new IntWritable();
    startSecInclusive = new LongWritable();
    endSecExclusive = new LongWritable();
    resultSize = new IntWritable();
    useRelRank = new BooleanWritable();
    containedWdeIds=new MapWritable();
  }

  public RelQuerySpecWritable(RelQuerySpec relspec) {
    centerNode = new BytesWritable(relspec.getCenterNode().getId());
    requiredRelType = new MapWritable();
    containedWdeIds=new MapWritable();
    if(relspec.getRequiredRelType()!=null)
    for (RelationType rel : relspec.getRequiredRelType())
      requiredRelType.put(new Text(rel.getStringForm()), NullWritable.get());
    if(relspec.getContainedWdeIds()!=null)
    	for(ByteArray wdeId:relspec.getContainedWdeIds())
    		containedWdeIds.put(new BytesWritable(wdeId.getBytes()), NullWritable.get());
    requiredAttribute = new IntWritable(relspec.getRequiredAttribute().getIntForm());
    startSecInclusive = new LongWritable(relspec.getTimeRange().getStartInclusiveInSec());
    endSecExclusive = new LongWritable(relspec.getTimeRange().getEndExclusiveInSec());
    resultSize = new IntWritable(relspec.getResultSize());
    useRelRank = new BooleanWritable(relspec.isRelRankEnabled());
  }

  public RelQuerySpec getRelQuerySpec(){
    Node node=new Node(Bytes.head(centerNode.getBytes(),centerNode.getLength()));
    RelQuerySpec.RelQuerySpecBuilder relbuilder = new RelQuerySpec.RelQuerySpecBuilder(node);
    relbuilder.attribute(Attribute.getAttribute(requiredAttribute.get()));
    for(Writable relType:requiredRelType.keySet())
      relbuilder.relType(RelationType.getType(((Text) relType).toString()));
    for(Writable wdeId:containedWdeIds.keySet())
    	relbuilder.wdeId(Bytes.head(((BytesWritable)wdeId).getBytes(), WdeRef.WDEID_SIZE));
    TimeRange range=new TimeRange(startSecInclusive.get(),endSecExclusive.get());
    relbuilder.timeRange(range);
    relbuilder.resultSize(resultSize.get());
    relbuilder.useRelRank(useRelRank.get());
    return relbuilder.build();
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    centerNode.readFields(in);
    
    requiredRelType.readFields(in);
    requiredAttribute.readFields(in);
    startSecInclusive.readFields(in);
    endSecExclusive.readFields(in);
    resultSize.readFields(in);
    useRelRank.readFields(in);
    containedWdeIds.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    centerNode.write(out);
    requiredRelType.write(out);
    requiredAttribute.write(out);
    startSecInclusive.write(out);
    endSecExclusive.write(out);
    resultSize.write(out);
    useRelRank.write(out);
    containedWdeIds.write(out);
  }
  
}
