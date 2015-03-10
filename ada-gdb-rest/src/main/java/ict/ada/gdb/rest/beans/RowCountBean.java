package ict.ada.gdb.rest.beans;

import ict.ada.common.model.NodeType.Attribute;
import ict.ada.gdb.rest.typemap.NodeTypeMapper;
import ict.ada.gdb.rowcounter.TableRowCount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowCountBean {
  // private Map<String,Long> tableRowCount;

  private String errorCode = "success";
  private long totalEdges;
  private long totalNodes;
  private List<Channel> channels;

  public RowCountBean() {
    this.channels = new ArrayList<Channel>();
  }

  public RowCountBean(TableRowCount rowCount) {
    this.totalEdges = rowCount.getTotalEdgeCount();
    this.totalNodes = rowCount.getTotalNodeCount();
    this.channels = new ArrayList<Channel>();
    for (Map.Entry<Integer, ict.ada.gdb.rowcounter.TableRowCount.Channel> e : rowCount
        .getChannelRowCount().entrySet()) {
      this.channels.add(new Channel(NodeTypeMapper
          .getChannelName(ict.ada.common.model.NodeType.Channel.getChannel(e.getKey())), e
          .getValue().getNode(), e.getValue().getEdge()));
    }

  }

  public void addChannel(String channelName, ict.ada.gdb.rowcounter.TableRowCount.Channel count) {
    this.channels.add(new Channel(channelName, count.getNode(), count.getEdge()));
  }

  public void setTotalEdges(long totalEdges) {
    this.totalEdges = totalEdges;
  }

  public void setTotalNodes(long totalNodes) {
    this.totalNodes = totalNodes;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public long getTotalEdges() {
    return totalEdges;
  }

  public long getTotalNodes() {
    return totalNodes;
  }

  public List<Channel> getChannels() {
    return channels;
  }

  public static class Channel {
    private String channel;
    private Map<String, Long> nodeTypeCount;
    private long edgeCount = 0L;

    public Channel(String channel, Map<Integer, Long> nodeCount, long edgeCount) {
      this.channel = channel;
      this.edgeCount = edgeCount;
      nodeTypeCount = new HashMap<String, Long>(nodeCount.size());
      for (Map.Entry<Integer, Long> e : nodeCount.entrySet())
        nodeTypeCount.put(NodeTypeMapper.getAttributeName(Attribute.getAttribute(e.getKey())),
            e.getValue());
    }

    public String getChannel() {
      return channel;
    }

    public Map<String, Long> getNodeTypeCount() {
      return nodeTypeCount;
    }

    public long getEdgeCount() {
      return edgeCount;
    }

  }
}
