package ict.ada.common.model;

import ict.ada.common.util.Hex;

import java.util.HashMap;
import java.util.Map;

public final class NodeType {
	public static final int NODETYPE_BYTES_SIZE = 2;
	private Channel channel;
	private Attribute attribute;
	private static Map<Integer, NodeType> intToNodeType;
	static {
		intToNodeType=new HashMap<Integer,NodeType>();
		for(Channel channel:Channel.values())
			for(Attribute attribute:Attribute.values())
				intToNodeType.put(channel.getIntForm()+attribute.getIntForm()*1000, new NodeType(channel,attribute));
	}
	private NodeType( Channel channel, Attribute attribute) {
		this.channel=channel;
		this.attribute=attribute;
	}
   
	/**
	 * Get NodeType enum by its integer form
	 * 
	 * @param intForm
	 * @return null if intForm is illegal
	 */
	public static NodeType getType(int intForm) {
		return intToNodeType.get(intForm);
	}

	/**
	 * Get NodeType enum by its bytes form.A normal type has 2 bytes, an
	 * aggregate type has 1 byte.
	 * 
	 * @param bytesForm
	 * @return
	 */
	public static NodeType getType(byte highByte,byte lowByte) {
		return intToNodeType.get((highByte&0xff)*1000+(lowByte&0xff));
	}
	public static NodeType getType(Channel channel,Attribute attribute) {
		return intToNodeType.get(channel.getIntForm()+attribute.getIntForm()*1000);
	}

	/**
	 * An "aggregate type"(e.g. ANY_PERSON) consists of more than one simple
	 * NodeType(e.g. BBS_PERSON).<br>
	 * Aggregate Type should only be used in a query.
	 * 
	 * @return
	 */
	public boolean isAggregateType() {
		return channel==Channel.ANY&&attribute==Attribute.ANY;
	}

	/**
	 * An normal type returns a 2-bytes array, and an aggregate type returns a
	 * 1-byte array.
	 * 
	 * @return
	 */
	public byte[] getBytesForm() {
		if(channel==Channel.ANY||attribute==Attribute.ANY)
			return new byte[0];
		return new byte[]{attribute.getByteFrom()[0],channel.getByteFrom()[0]};
	}

	public int getIntegerForm() {
		return channel.getIntForm()+attribute.getIntForm()*1000;
	}
	
	public int getIntegerForm1() {
		return channel.getIntForm()*1000+attribute.getIntForm();
	}
	public String getStringForm() {
		return Hex.encodeHex(getBytesForm());
	}
    
	public Channel getChannel() {
		return channel;
	}

	public Attribute getAttribute() {
		return attribute;
	}
     /**
      *  add a channel steps
      * 1 add a  channel in {@link Channel},
      * 2, complete in {@link ict.ada.gdb.schema.GdbHTablePartitionPolicy.getPartitionName(Channel type)}
      * 3, complete in ict.ada.gdb.rest.typemap.NodeTypeMapper[strToChannel ,channelToStr]
      * 
      * */
	public static enum Channel {
		NEWS(1),
		BBS(2),
		BLOG(3),
		WEIBO(5),
		TWITTER(6),
		WEB(8),
		WIKIBAIKE(50),
		BAIDUBAIKE(51),
		HUDONGBAIKE(52),
		LINKEDIN(53),
		SCHOLAR(100),
		DATA973(120),
		COMPUTE(200),
		KNOWLEDGE(208),
		DATA(209),
		MERGE(211),
		EL(212),
		ER(213),
		ANY(255);
		private int intForm;
		private byte[] byteForm;
		private static Map<Integer, Channel> intToChannel;
		static {
			intToChannel = new HashMap<Integer, Channel>();
			for (Channel type : Channel.values()) {
				intToChannel.put(type.intForm, type);
			}
		}

		private Channel(int type) {
			this.intForm = type;
			if(intForm==255)
				byteForm= new byte[0];
			else byteForm=new byte[]{(byte) (intForm  & 0xff)};
		}

		public static Channel getChannel(int type) {
			return intToChannel.get(type);
		}
		public byte[] getByteFrom(){
			return byteForm;
		}
		public  int getIntForm(){
			return intForm;
		}
	}

	/**
	 * add a Attribute steps:
	 * 1,  add a  Attribute in {@link Attribute},
	 * 2,complete in {@link ict.ada.gdb.rest.typemap.NodeTypeMapper[strToAttribute ,attributeToStr]}
	 * */
	public static enum Attribute {
		PERSON(1),
		ACCOUNT(2),
		ORG(21),
		LOCATION(31),
		TIME(41),
		LAW(51),
		WEAPON(52),
		EMAIL(101),
		TELEPHONE(102),
		QQ(103),
		IP(199),
		EVENT(201),
		CATEGORY(202),
		ANY(255);
		private int intForm;
		private byte[] byteForm;
		private static Map<Integer, Attribute> intToAttribute;
		static {
			intToAttribute = new HashMap<Integer, Attribute>();
			for (Attribute type : Attribute.values()) {
				intToAttribute.put(type.intForm, type);
			}

		}

		private Attribute(int type) {
			this.intForm = type;
			if(intForm==255)
				byteForm= new byte[0];
			else byteForm=new byte[]{(byte) (intForm  & 0xff)};
		}

		public static Attribute getAttribute(int type) {
			return intToAttribute.get(type);
		}
		public byte[] getByteFrom(){
			return byteForm;
		}
		public  int getIntForm(){
			return intForm;
		}
		
	}
	@Override
	public String toString(){
		return channel.toString()+"_"+attribute.toString();
	}
	public static void main(String[] args){
		byte high=1;
		byte low = 8;
		System.out.println(NodeType.getType(high, low));
		
	}
}
