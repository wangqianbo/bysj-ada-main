package ict.ada.gdb.dataloader;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.dataloader.DataOperation.DataOpType;
import ict.ada.gdb.dataloader.GdbJsonDataProcessor.DataOpHandler;
import ict.ada.gdb.service.AdaGdbService;

import java.io.IOException;
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
public class GdbJsonDataProcessor {
  // private static final Log LOG = LogFactory.getLog(GdbJsonDataProcessor.class);

  /**
   * Define how to handle each DataOperation in GdbJsonDataProcessor
   */
  public interface DataOpHandler {
    public void handle(DataOperation dataOp) throws GdbException;

    public void onFinish() throws GdbException;
  }

  private DataOpHandler handler;
  private ObjectMapper jacksonMapper;

  public GdbJsonDataProcessor(DataOpHandler handler) {
    if (handler == null) throw new NullPointerException("null handler");
    this.handler = handler;
    jacksonMapper = new ObjectMapper();
    jacksonMapper.configure(DeserializationConfig.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
  }

  /**
   * Create GdbJsonDataProcessor with DefaultDataOpHandler, which write data into GDB
   */
  public GdbJsonDataProcessor() {
    this(new DefaultDataOpHandler());
  }

  /**
   * Process one JSON data line
   * 
   * @param dataLine
   * @throws GdbDataLoaderException
   */
  public void process(String dataLine) throws GdbDataLoaderException {
    dataLine = dataLine.trim();
    if (dataLine == null || dataLine.length() < 2) {
      throw new GdbDataLoaderException("Corrupted data.  dataline=[" + dataLine + "]");
    }
    DataOpType type = DataOpType.fromChar(dataLine.charAt(0));
    if (type == null) {
      throw new GdbDataLoaderException("Illegal DataOpType char=[" + dataLine.charAt(0) + "] "
          + " dataline=[" + dataLine + "]");
    }
    // one type char, one \t char, and then json. So here is substring(2)
    List<DataOperation> opList = generateDataOperation(type, dataLine.substring(2));
    for (DataOperation op : opList) {
      try {
        handler.handle(op);
      } catch (GdbException e) {
        throw new GdbDataLoaderException("GdbException caused failure. " + " dataline=[" + dataLine
            + "]", e);
      }
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

  private List<DataOperation> generateDataOperation(DataOpType opType, String jsonData)
      throws GdbDataLoaderException {
    switch (opType) {
    case ADD_EDGE:
      EdgeBean edgeBean = parseEdgeBean(jsonData);
      List<Edge> edges = EdgeBean.convert(edgeBean);
      List<DataOperation> ops = new ArrayList<DataOperation>(edges.size());
      for (Edge edge : edges) {
        ops.add(new DataOperation(DataOpType.ADD_EDGE, edge));
      }
      return ops;
    case ADD_NODE:
      NodeBean nodeBean = parseNodeBean(jsonData);
      Node node = NodeBean.convert(nodeBean);
      return Collections.singletonList(new DataOperation(DataOpType.ADD_NODE, node));
    default:
      throw new IllegalStateException("Unknown DataOpType. opType=" + opType);
    }
  }

  private EdgeBean parseEdgeBean(String jsonData) throws GdbDataLoaderException {
    try {
      return jacksonMapper.readValue(jsonData, EdgeBean.class);
    } catch (IOException e) {
      throw new GdbDataLoaderException("JSON parser failed. jsonData=[" + jsonData + "]", e);
    }
  }

  private NodeBean parseNodeBean(String jsonData) throws GdbDataLoaderException {
    try {
      return jacksonMapper.readValue(jsonData, NodeBean.class);
    } catch (IOException e) {
      throw new GdbDataLoaderException("JSON parser failed. jsonData=[" + jsonData + "]", e);
    }
  }

}

/**
 * Default implementation. Write each DataOperation into GDB.
 */
class DefaultDataOpHandler implements DataOpHandler {

  private AdaGdbService gdb = new AdaGdbService();

  @Override
  public void handle(DataOperation dataOp) throws GdbException {
    switch (dataOp.getType()) {
    // use GDB Java interface
    case ADD_NODE:
      Node node = (Node) dataOp.getData();
      gdb.addNode(node);
      break;
    case ADD_EDGE:
      Edge edge = (Edge) dataOp.getData();
//      gdb.addEdge(edge);
      gdb.addEdgeWithComputation(edge);
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
