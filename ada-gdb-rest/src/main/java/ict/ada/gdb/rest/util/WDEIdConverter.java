package ict.ada.gdb.rest.util;

import java.util.HashMap;
import java.util.Map;

//author wangqianbo
public class WDEIdConverter {
  public enum ChannelType {
    NEWS(1, "新闻"), BBS(2, "BBS"), BLOG(3, "Blog"), FOREIGNNEWS(4, "境外新闻"), DOMESTICMICROBLOGS(5,
        "境内微博"), FOREIGNMICROBLOGS(6, "境外微博"), COMMENT(7, "回帖评论"), MEATASEARCH(8, "元搜索"), ELECNEWSPAPERS(
        9, "电子报纸"),
    // wikipedia
    WIKIPEDIA(50, "维基百科"), EWIKIPEDIA(50, "维基百科"),
    // Baidu Encyclopedia
    BAIDUPEDIA(51, "百度百科"), HUDONGPEDIA(52, "互动百科"), LINKEDIN(53, "linkedin"),
    // Academic
    ACADEMIC(100, "学术"), ORGSITE(100, "机构"), MATERIAL(201, "资料"), CACULATION(200, "计算"), EVENT(202,
        "事件"),KNOWLEGE(208,"kownledge"),DATA973(120,"973data"),MERGE(211,"merge");

    private static Map<Integer, ChannelType> intToChannelType;
    static {
      intToChannelType = new HashMap<Integer, ChannelType>();
      for (ChannelType type : ChannelType.values()) {
        intToChannelType.put(type.getChannelType(), type);
      }
    }
    private int channelType;
    private String channelName;

    private ChannelType(int channelType, String channelName) {
      this.channelType = channelType;
      this.channelName = channelName;
    }

    public int getChannelType() {
      return this.channelType;
    }

    public String getChannelName() {
      return this.channelName;
    }

    public static String getChannelName(int channelType) throws IllegalArgumentException {
      try {
        return intToChannelType.get(channelType).getChannelName();
      } catch (Exception e) {
        throw new IllegalArgumentException("the channel type [intformat : " + channelType
            + " ] not exists");
      }
    }
    public static String getChannelNameAll(int channelType)  {
      try {
        return intToChannelType.get(channelType).getChannelName();
      } catch (Exception e) {
         return "unkown";
      }
    }
  }
  public static String getChannel(byte[] WDEId) throws IllegalArgumentException {
    try {
      return ChannelType.getChannelName((int) (WDEId[1] & 0xff));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("the wdeId [" + NodeIdConveter.toString(WDEId)
          + "] has a problem" + e.getMessage());
    }
  }
  public static String getChannelIgnoreException(byte[] WDEId)  {
   
      return ChannelType.getChannelNameAll((int) (WDEId[1] & 0xff));
   
  }
  public static void checkWDEId(String wdeid) {

  }

  public static long bytes2long(byte[] b) {
    long temp = 0;
    long res = 0;
    for (int i = 0; i < 8; i++) {
      res <<= 8;
      temp = b[i] & 0xff;
      res |= temp;
    }
    return res;
  }

  public static byte[] long2bytes(long num) {
    byte[] b = new byte[8];
    for (int i = 0; i < 8; i++) {
      b[i] = (byte) (num >>> (56 - (i * 8)));
    }
    return b;
  }

  public static void main(String[] args) {
    byte a = (byte) 0x5b;
    System.out.println(a & 0xff);
    System.out.println(ChannelType.getChannelName((int) (a & 0xff)));
  }
}
