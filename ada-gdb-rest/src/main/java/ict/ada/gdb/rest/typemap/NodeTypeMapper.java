package ict.ada.gdb.rest.typemap;

import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

public class NodeTypeMapper {
  private static LinkedHashMap<String, Channel> strToChannel = new LinkedHashMap<String, Channel>();
  private static LinkedHashMap<String, Attribute> strToAttribute = new LinkedHashMap<String, Attribute>();

  private static LinkedHashMap<Channel, String> channelToStr = new LinkedHashMap<Channel, String>();
  private static LinkedHashMap<Attribute, String> attributeToStr = new LinkedHashMap<Attribute, String>();

  static {
    strToChannel.put("merge", Channel.MERGE);
    
    strToChannel.put("knowledge", Channel.KNOWLEDGE);
    
    strToChannel.put("web", Channel.WEB);
    
    strToChannel.put("linkedin", Channel.LINKEDIN);
    
    strToChannel.put("wiki_baike", Channel.WIKIBAIKE);
    
    strToChannel.put("hudong_baike", Channel.HUDONGBAIKE);
    
    strToChannel.put("baidu_baike", Channel.BAIDUBAIKE);

    strToChannel.put("BBS", Channel.BBS);

    strToChannel.put("blog", Channel.BLOG);

    strToChannel.put("news", Channel.NEWS);

    strToChannel.put("scholar", Channel.SCHOLAR);

    strToChannel.put("twitter", Channel.TWITTER);

    strToChannel.put("weibo", Channel.WEIBO);
    
    strToChannel.put("data973", Channel.DATA973);
    
    strToChannel.put("compute", Channel.COMPUTE);
    
    strToChannel.put("data", Channel.DATA);
    
    strToChannel.put("el", Channel.EL);
    
    strToChannel.put("er", Channel.ER);

    strToAttribute.put("person", Attribute.PERSON);

    strToAttribute.put("account", Attribute.ACCOUNT);

    strToAttribute.put("org", Attribute.ORG);

    strToAttribute.put("time", Attribute.TIME);

    strToAttribute.put("law", Attribute.LAW);

    strToAttribute.put("weapon", Attribute.WEAPON);

    strToAttribute.put("email", Attribute.EMAIL);

    strToAttribute.put("telephone", Attribute.TELEPHONE);

    strToAttribute.put("qq", Attribute.QQ);

    strToAttribute.put("ip", Attribute.IP);

    strToAttribute.put("event", Attribute.EVENT);

    strToAttribute.put("category", Attribute.CATEGORY);

    strToAttribute.put("location", Attribute.LOCATION);
    for (Entry<String, Channel> e : strToChannel.entrySet())
      channelToStr.put(e.getValue(), e.getKey());
    for (Entry<String, Attribute> e : strToAttribute.entrySet())
      attributeToStr.put(e.getValue(), e.getKey());
  }

  public static Attribute getAttribute(String name) {
    Attribute attribute = strToAttribute.get(name);
    if (attribute == null) throw new IllegalArgumentException("illegal attributeName : " + name);
    return attribute;
  }

  public static Channel getChannel(String name) {
    Channel channel = strToChannel.get(name);
    if (channel == null) throw new IllegalArgumentException("illegal channelName : " + name);
    return channel;
  }

  public static String getAttributeName(Attribute attribute) {
    return attributeToStr.get(attribute);
  }

  public static String getChannelName(Channel channel) {
    return channelToStr.get(channel);
  }

  public static Collection<Channel> getMapperChannels() {
    return channelToStr.keySet();
  }

  public static Collection<Attribute> getMapperAttributes() {
    return attributeToStr.keySet();
  }
  public static void main(String[] args){
    System.out.println(NodeTypeMapper.getMapperChannels());
  }
}
