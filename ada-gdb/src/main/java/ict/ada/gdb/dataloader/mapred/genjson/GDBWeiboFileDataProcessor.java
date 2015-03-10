package ict.ada.gdb.dataloader.mapred.genjson;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.common.model.NodeAttribute;
import ict.ada.common.model.NodeAttribute.AttrValueInfo;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.common.model.Relation;
import ict.ada.common.model.RelationType;
import ict.ada.common.model.WdeRef;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.dataloader.GdbDataLoaderException;
import ict.ada.gdb.dataloader.mapred.genjson.GDBWeiboFileDataProcessor.DataOpHandler;
import ict.ada.gdb.dataloader.mapred.genjson.WeiboDataOperation.DataOpType;
import ict.ada.gdb.service.AdaGdbService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 
 * Processor for GDB JSON Data
 * 
 */
public class GDBWeiboFileDataProcessor {
  // private static final Log LOG = LogFactory.getLog(GdbJsonDataProcessor.class);
private final static byte[] WDEID= new byte[WdeRef.WDEID_SIZE];
  /**
   * Define how to handle each DataOperation in GdbJsonDataProcessor
   */
  public interface DataOpHandler {
    public void handle(WeiboDataOperation dataOp) throws GdbException;

    public void onFinish() throws GdbException;
  }

  private DataOpHandler handler;
  private ObjectMapper jacksonMapper;

  public GDBWeiboFileDataProcessor(DataOpHandler handler) {
    if (handler == null) throw new NullPointerException("null handler");
    this.handler = handler;
    jacksonMapper = new ObjectMapper();
    jacksonMapper.configure(DeserializationConfig.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
  }

  /**
   * Create GdbJsonDataProcessor with DefaultDataOpHandler, which write data into GDB
   */
  public GDBWeiboFileDataProcessor() {
    this(new DefaultDataOpHandler());
  }

  /**
   * Process one JSON data line
   * 
   * @param dataLine
   * @throws GdbDataLoaderException
   */
  public void process(char type, String dataLine) throws GdbDataLoaderException {
    dataLine = dataLine.trim();
    if (dataLine == null || dataLine.length() < 2) {
      throw new GdbDataLoaderException("Corrupted data.  dataline=[" + dataLine + "]");
    }
    DataOpType opType = DataOpType.fromChar(type);
    if (opType == null) {
      throw new GdbDataLoaderException("Illegal DataOpType char=[" + dataLine.charAt(0) + "] "
          + " dataline=[" + dataLine + "]");
    }
    // one type char, one \t char, and then json. So here is substring(2)
    List<WeiboDataOperation> ops = generateDataOperation(opType, dataLine);
    
      try {
    	  for(WeiboDataOperation op : ops){
    		  handler.handle(op);
    	  }
      } catch (GdbException e) {
        throw new GdbDataLoaderException("GdbException caused failure. " + " dataline=[" + dataLine
            + "]", e);
      }
  }

  /**
   * Finish using this processor. DataOpHandler's onFinish() will be called here.<br>
   * You MUST call this method to notify underlying cache structures to flush commits.
   * 
   * @throws GdbDataLoaderException
   */
  public void close() throws GdbDataLoaderException {
    try {
      this.handler.onFinish();
    } catch (GdbException e) {
      throw new GdbDataLoaderException("GdbException when tried to close GdbJsonDataProcessor.", e);
    }
  }

  private List<WeiboDataOperation> generateDataOperation(DataOpType opType, String dataLine)
      throws GdbDataLoaderException {
    switch (opType) {
    case ADD_EDGE:
      List<Edge> edges = parseEdge(dataLine);
      List<WeiboDataOperation> ops = new ArrayList<WeiboDataOperation>(edges.size());
      for(Edge edge : edges){
    	  ops.add(new WeiboDataOperation(opType,edge));
      }
      return ops;
      
    case ADD_NODE:
      Node node =parseNode(dataLine);
      return Collections.singletonList(new WeiboDataOperation(opType,node));
    default:
      throw new IllegalStateException("Unknown DataOpType. opType=" + opType);
    }
  }
  
  private List<Edge> parseEdge(String dataLine) throws GdbDataLoaderException {
	  String[] segs = dataLine.split(",");
	  if(segs.length != 4){
       	  throw new GdbDataLoaderException("Corrupted data.  dataline=[" + dataLine + "]");
         }
	  Node node1 = new Node(NodeType.getType(Channel.WEIBO, Attribute.ACCOUNT),segs[0]);
	  Node node2 = new Node(NodeType.getType(Channel.WEIBO, Attribute.ACCOUNT),segs[1]);
	  Edge edge = new Edge(node1,node2);
	  Edge auxEdge = new Edge(node2,node1);
	  Relation rel = new Relation(edge,RelationType.getType(segs[2]));
	  rel.setWdeRefs(Collections.singletonList(new WdeRef(WDEID, 0, 1,Integer.parseInt(segs[3]))));
	  edge.addRelation(rel);
	  List<Edge>result = new ArrayList<Edge>(2);
	  result.add(edge);
	  result.add(auxEdge);
	  return result;
  }

  private Node parseNode(String dataLine) throws GdbDataLoaderException {
    	 String[] segs = dataLine.split(",");
         if(segs.length != 6){
       	  throw new GdbDataLoaderException("Corrupted data.  dataline=[" + dataLine + "]");
         }
         Node node = new Node(NodeType.getType(Channel.WEIBO, Attribute.ACCOUNT),segs[0]);
         List<String> snames = new ArrayList<String>(6);
         snames.add(segs[1]);
         NodeAttribute ts = new NodeAttribute("ts",Collections.singletonList(new AttrValueInfo(segs[2],null)));
         node.addNodeAttribute(ts);
         snames.add(segs[2]);
         NodeAttribute loc = new NodeAttribute("loc",Collections.singletonList(new AttrValueInfo(segs[3],null)));
         node.addNodeAttribute(loc);
         snames.add(segs[3]);
         NodeAttribute sex = new NodeAttribute("sex",Collections.singletonList(new AttrValueInfo(segs[4],null)));
         node.addNodeAttribute(sex);
         snames.add(segs[4]);
         NodeAttribute age = new NodeAttribute("age",Collections.singletonList(new AttrValueInfo(segs[5],null)));
         node.addNodeAttribute(age);
         snames.add(segs[5]);
         node.setSnames(snames);
        return node;
  }

}

/**
 * Default implementation. Write each DataOperation into GDB.
 */
class DefaultDataOpHandler implements DataOpHandler {

  private AdaGdbService gdb = new AdaGdbService();

  @Override
  public void handle(WeiboDataOperation dataOp) throws GdbException {
    switch (dataOp.getType()) {
    // use GDB Java interface
    case ADD_NODE:
      Node node = (Node) dataOp.getData();
      gdb.addNode(node);
      break;
    case ADD_EDGE:
      Edge edge = (Edge) dataOp.getData();
      gdb.addEdge(edge);
      break;
    default:
      throw new IllegalStateException("Unknown DataOpType");
    }
  }

  @Override
  public void onFinish() throws GdbException {
    gdb.finishUpdate();
  }
}
