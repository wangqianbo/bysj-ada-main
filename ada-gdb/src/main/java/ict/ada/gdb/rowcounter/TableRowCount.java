package ict.ada.gdb.rowcounter;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

public class TableRowCount {

	private Map<Integer, Channel> channelRowCount ;
	private long totalNodeCount = 0L;
	private long totalEdgeCount = 0L;
    public TableRowCount(){
    	
    	this.channelRowCount=new HashMap<Integer, Channel>();
    }
	
	public void addChannel(int channel, Map<Integer, Long> nodeCount,
			Long edgeCount) {
		for (Map.Entry<Integer, Long> e : nodeCount.entrySet())
			this.totalNodeCount += e.getValue();
		this.totalEdgeCount += edgeCount;
		Channel channelResult = new Channel(nodeCount, edgeCount);
		channelRowCount.put(channel, channelResult);
	}

	public Map<Integer, Channel> getChannelRowCount() {
		return channelRowCount;
	}

	public long getTotalNodeCount() {
		return totalNodeCount;
	}

	public long getTotalEdgeCount() {
		return totalEdgeCount;
	}

	public void setChannelRowCount(Map<Integer, Channel> channelRowCount) {
		this.channelRowCount = channelRowCount;
	}

	public void setTotalNodeCount(long totalNodeCount) {
		this.totalNodeCount = totalNodeCount;
	}

	public void setTotalEdgeCount(long totalEdgeCount) {
		this.totalEdgeCount = totalEdgeCount;
	}

	public static class Channel {
		private Map<Integer, Long> node;
		private long edge;
        
		public Channel(){
			node=new HashMap<Integer,Long>();
			edge=0L;
		}
		
		public Channel(Map<Integer, Long> node, long edge) {
			this.node = node;
			this.edge = edge;
		}

		public Map<Integer, Long> getNode() {
			return node;
		}

		public long getEdge() {
			return edge;
		}

		public void setNode(Map<Integer, Long> node) {
			this.node = node;
		}

		public void setEdge(Long edge) {
			this.edge = edge;
		}

	}

	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException{
		String a = "a"+"\u0001"+"b"+"\u0001";
		System.out.println(a.substring(0,a.length()-1));
		System.out.println(a.length());
		
	}
}
