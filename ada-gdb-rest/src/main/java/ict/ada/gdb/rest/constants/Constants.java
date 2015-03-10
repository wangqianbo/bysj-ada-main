package ict.ada.gdb.rest.constants;

import java.util.HashMap;
import java.util.Map;

public class Constants {

  static Map<String, String> nodeXmls = new HashMap<String, String>();
  static Map<String, String> relXmls = new HashMap<String, String>();
  static Map<String, String> mapXmls = new HashMap<String, String>();

  private final static String simpleNodeXml = "<root errorCode=\"0\">"
      + "<node id=\"67ec8d4cad050b7d86974762227403c7\" name=\"程学旗\" type=\"4\"/>" + "</root>";

  private final static String noSuchNodeXml = "<root errorCode=\"0\">"
      + "<node id=\"\" name=\"\" type=\"4\"/>" + "</root>";

  private final static String simpleRelXml = "<root errorCode=\"0\">"
      + "<node id=\"67ec8d4cad050b7d86974762227403c7\" name=\"程学旗\" type=\"4\" weight=\"170\" parentId=\"67ec8d4cad050b7d86974762227403c7\"/>"
      + "<node id=\"c42a30607f3dc25f03af7d27f178b0b1\" name=\"李国杰\" type=\"4\" weight=\"60\" parentId=\"67ec8d4cad050b7d86974762227403c7\"/>"
      + "<node id=\"c42a30607f3dc25f03af7d27f178b0b1\" name=\"李国杰\" type=\"4\" weight=\"60\" parentId=\"67ec8d4cad050b7d86974762227403c7\"/>"
      + "<node id=\"58004b7036ed8cffceb608b23a2cd66b\" name=\"孙凝晖\" type=\"4\" weight=\"80\" parentId=\"67ec8d4cad050b7d86974762227403c7\"/>"
      + "<node id=\"09ebd8a2b052f975bbb098b16c12239c\" name=\"刘新宇\" type=\"4\" weight=\"130\" parentId=\"67ec8d4cad050b7d86974762227403c7\"/>"
      + "<node id=\"957feb6466015916d437b0d2c0f80e71\" name=\"罗瑞丽\" type=\"4\" weight=\"20\" parentId=\"67ec8d4cad050b7d86974762227403c7\"/>"
      + "<node id=\"00b04cccf9dc9b1c4209a793fb8fb80e\" name=\"岳强\" type=\"4\" weight=\"120\" parentId=\"67ec8d4cad050b7d86974762227403c7\"/>"
      + "<node id=\"e092d8a4dae3a201c6e18dc4b40c04a8\" name=\"周荣\" type=\"4\" weight=\"70\" parentId=\"67ec8d4cad050b7d86974762227403c7\"/>"
      + "<edge id=\"c42a30607f3dc25f03af7d27f178b0b1\" fromId=\"67ec8d4cad050b7d86974762227403c7\" toId=\"c42a30607f3dc25f03af7d27f178b0b1\" type=\"4\" weight=\"1\"/>"
      + "<edge id=\"c42a30607f3dc25f03af7d27f178b0b2\" fromId=\"67ec8d4cad050b7d86974762227403c7\" toId=\"58004b7036ed8cffceb608b23a2cd66b\" type=\"4\" weight=\"1\"/>"
      + "<edge id=\"c42a30607f3dc25f03af7d27f178b0b3\" fromId=\"67ec8d4cad050b7d86974762227403c7\" toId=\"58004b7036ed8cffceb608b23a2cd66b\" type=\"4\" weight=\"1\"/>"
      + "<edge id=\"c42a30607f3dc25f03af7d27f178b0b4\" fromId=\"67ec8d4cad050b7d86974762227403c7\" toId=\"09ebd8a2b052f975bbb098b16c12239c\" type=\"4\" weight=\"1\"/>"
      + "<edge id=\"c42a30607f3dc25f03af7d27f178b0b5\" fromId=\"67ec8d4cad050b7d86974762227403c7\" toId=\"09ebd8a2b052f975bbb098b16c12239c\" type=\"4\" weight=\"1\"/>"
      + "<edge id=\"c42a30607f3dc25f03af7d27f178b0b6\" fromId=\"67ec8d4cad050b7d86974762227403c7\" toId=\"957feb6466015916d437b0d2c0f80e71\" type=\"4\" weight=\"1\"/>"
      + "<edge id=\"c42a30607f3dc25f03af7d27f178b0b7\" fromId=\"67ec8d4cad050b7d86974762227403c7\" toId=\"00b04cccf9dc9b1c4209a793fb8fb80e\" type=\"4\" weight=\"1\"/>"
      + "<edge id=\"c42a30607f3dc25f03af7d27f178b0b8\" fromId=\"67ec8d4cad050b7d86974762227403c7\" toId=\"e092d8a4dae3a201c6e18dc4b40c04a8\" type=\"4\" weight=\"1\"/>"
      + "</root>";

  private static final String simpleMapXml = "<root errorCode=\"0\">"
      + "<map id=\"857feb6466015916d437b0d2c0f80e71\" nodeId=\"67ec8d4cad050b7d86974762227403c7\" lat=\"116.46\" lon=\"39.92\"/>"
      + "<map id=\"857feb6466015916d437b0d2c0f80e72\" nodeId=\"c42a30607f3dc25f03af7d27f178b0b1\" lat=\"117.1\" lon=\"40.13\"/>"
      + "<map id=\"857feb6466015916d437b0d2c0f80e73\" nodeId=\"58004b7036ed8cffceb608b23a2cd66b\" lat=\"116.46\" lon=\"39.92\"/>"
      + "<map id=\"857feb6466015916d437b0d2c0f80e74\" nodeId=\"09ebd8a2b052f975bbb098b16c12239c\" lat=\"116.46\" lon=\"39.92\"/>"
      + "<map id=\"857feb6466015916d437b0d2c0f80e75\" nodeId=\"09ebd8a2b052f975bbb098b16c12239c\" lat=\"117.1\" lon=\"40.13\"/>"
      + "<map id=\"857feb6466015916d437b0d2c0f80e76\" nodeId=\"957feb6466015916d437b0d2c0f80e71\" lat=\"116.46\" lon=\"40.13\"/>"
      + "<map id=\"857feb6466015916d437b0d2c0f80e77\" nodeId=\"00b04cccf9dc9b1c4209a793fb8fb80e\" lat=\"117.1\" lon=\"39.92\"/>"
      + "<map id=\"857feb6466015916d437b0d2c0f80e78\" nodeId=\"e092d8a4dae3a201c6e18dc4b40c04a8\" lat=\"117.1\" lon=\"40.13\"/>"
      + "</root>";

  private final static String noSuchMapXml = "<root errorCode=\"404\">"
      + "<map id=\"\" nodeId=\"\" lat=\"\" lon=\"\"/>" + "</root>";

  /*
   * private final static String threeDescendentsXml = "<root errorCode=\"0\">" +
   * "<node id=\"1001\" name=\"A100\" type=\"1\" weight=\"330\" parentId=\"1001\"/>" +
   * "<node id=\"2000\" name=\"A101\" type=\"1\" weight=\"200\" parentId=\"1001\"/>" +
   * "<edge id=\"2100\" fromId=\"1001\" toId=\"2000\" type=\"1\" weight=\"3\"/>" +
   * "<node id=\"2002\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"2000\"/>" +
   * "<node id=\"2003\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"2000\"/>" +
   * "<node id=\"2004\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"2000\"/>" +
   * "<node id=\"2005\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"2000\"/>" +
   * "<node id=\"2006\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"2000\"/>" +
   * "<edge id=\"2101\" fromId=\"2000\" toId=\"2002\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"2102\" fromId=\"2000\" toId=\"2003\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"2103\" fromId=\"2000\" toId=\"2004\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"2104\" fromId=\"2000\" toId=\"2005\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"2105\" fromId=\"2000\" toId=\"2006\" type=\"1\" weight=\"1\"/>" +
   * "<node id=\"3000\" name=\"A101\" type=\"1\" weight=\"200\" parentId=\"1001\"/>" +
   * "<edge id=\"3100\" fromId=\"1001\" toId=\"3000\" type=\"1\" weight=\"3\"/>" +
   * "<node id=\"3002\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"3000\"/>" +
   * "<node id=\"3003\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"3000\"/>" +
   * "<node id=\"3004\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"3000\"/>" +
   * "<node id=\"3005\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"3000\"/>" +
   * "<node id=\"3006\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"3000\"/>" +
   * "<edge id=\"3101\" fromId=\"3000\" toId=\"3002\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"3102\" fromId=\"3000\" toId=\"3003\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"3103\" fromId=\"3000\" toId=\"3004\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"3104\" fromId=\"3000\" toId=\"3005\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"3105\" fromId=\"3000\" toId=\"3006\" type=\"1\" weight=\"1\"/>" +
   * "<node id=\"4000\" name=\"A101\" type=\"4\" weight=\"200\" parentId=\"1001\"/>" +
   * "<edge id=\"4100\" fromId=\"1001\" toId=\"4000\" type=\"4\" weight=\"1\"/>" +
   * "<node id=\"4002\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"4000\"/>" +
   * "<node id=\"4003\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"4000\"/>" +
   * "<node id=\"4004\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"4000\"/>" +
   * "<node id=\"4005\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"4000\"/>" +
   * "<node id=\"4006\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"4000\"/>" +
   * "<edge id=\"4101\" fromId=\"4000\" toId=\"4002\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"4102\" fromId=\"4000\" toId=\"4003\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"4103\" fromId=\"4000\" toId=\"4004\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"4104\" fromId=\"4000\" toId=\"4005\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"4105\" fromId=\"4000\" toId=\"4006\" type=\"4\" weight=\"1\"/>" +
   * "<node id=\"5000\" name=\"A101\" type=\"4\" weight=\"200\" parentId=\"1001\"/>" +
   * "<edge id=\"5100\" fromId=\"1001\" toId=\"5000\" type=\"4\" weight=\"2\"/>" +
   * "<node id=\"5002\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"5000\"/>" +
   * "<node id=\"5003\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"5000\"/>" +
   * "<node id=\"5004\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"5000\"/>" +
   * "<node id=\"5005\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"5000\"/>" +
   * "<node id=\"5006\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"5000\"/>" +
   * "<node id=\"5007\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"5000\"/>" +
   * "<node id=\"5008\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"5000\"/>" +
   * "<edge id=\"5101\" fromId=\"5000\" toId=\"5002\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"5102\" fromId=\"5000\" toId=\"5003\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"5103\" fromId=\"5000\" toId=\"5004\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"5104\" fromId=\"5000\" toId=\"5005\" type=\"4\" weight=\"3\"/>" +
   * "<edge id=\"5105\" fromId=\"5000\" toId=\"5006\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"5106\" fromId=\"5000\" toId=\"5007\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"5107\" fromId=\"5000\" toId=\"5008\" type=\"4\" weight=\"1\"/>" +
   * "<node id=\"6000\" name=\"A101\" type=\"4\" weight=\"200\" parentId=\"1001\"/>" +
   * "<edge id=\"6100\" fromId=\"1001\" toId=\"6000\" type=\"4\" weight=\"3\"/>" +
   * "<node id=\"6002\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"6000\"/>" +
   * "<node id=\"6003\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"6000\"/>" +
   * "<node id=\"6004\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"6000\"/>" +
   * "<node id=\"6005\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"6000\"/>" +
   * "<node id=\"6006\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"6000\"/>" +
   * "<edge id=\"6101\" fromId=\"6000\" toId=\"6002\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"6102\" fromId=\"6000\" toId=\"6003\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"6103\" fromId=\"6000\" toId=\"6004\" type=\"4\" weight=\"3\"/>" +
   * "<edge id=\"6104\" fromId=\"6000\" toId=\"6005\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"6105\" fromId=\"6000\" toId=\"6006\" type=\"4\" weight=\"1\"/>" +
   * "<node id=\"7000\" name=\"A101\" type=\"4\" weight=\"200\" parentId=\"1001\"/>" +
   * "<edge id=\"7100\" fromId=\"1001\" toId=\"7000\" type=\"4\" weight=\"3\"/>" +
   * "<node id=\"7002\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"7000\"/>" +
   * "<node id=\"7003\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"7000\"/>" +
   * "<node id=\"7004\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"7000\"/>" +
   * "<node id=\"7005\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"7000\"/>" +
   * "<edge id=\"7101\" fromId=\"7000\" toId=\"7002\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"7102\" fromId=\"7000\" toId=\"7003\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"7103\" fromId=\"7000\" toId=\"7004\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"7104\" fromId=\"7000\" toId=\"7005\" type=\"4\" weight=\"1\"/>" + "</root>";
   */

  /*
   * private final static String twoRelationXml = "<root errorCode=\"0\">" +
   * "<node id=\"1009\" name=\"A000\" type=\"4\" weight=\"100\" parentId=\"1009\"/>" +
   * "<node id=\"1001\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"1009\"/>" +
   * "<node id=\"1002\" name=\"A102\" type=\"1\" weight=\"80\" parentId=\"1009\"/>" +
   * "<node id=\"1003\" name=\"A101\" type=\"4\" weight=\"60\" parentId=\"1009\"/>" +
   * "<node id=\"1004\" name=\"A102\" type=\"1\" weight=\"80\" parentId=\"1009\"/>" +
   * "<edge id=\"1100\" fromId=\"1009\" toId=\"1001\" type=\"4\" weight=\"1\"/>" +
   * "<edge id=\"1101\" fromId=\"1009\" toId=\"1002\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"1102\" fromId=\"1009\" toId=\"1003\" type=\"4\" weight=\"3\"/>" +
   * "<edge id=\"1103\" fromId=\"1009\" toId=\"1004\" type=\"1\" weight=\"2\"/>" +
   * "<node id=\"2000\" name=\"A000\" type=\"1\" weight=\"100\" parentId=\"2000\"/>" +
   * "<node id=\"2001\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"2000\"/>" +
   * "<node id=\"2002\" name=\"A102\" type=\"2\" weight=\"80\" parentId=\"2000\"/>" +
   * "<node id=\"2003\" name=\"A101\" type=\"1\" weight=\"60\" parentId=\"2000\"/>" +
   * "<node id=\"2004\" name=\"A102\" type=\"3\" weight=\"80\" parentId=\"2000\"/>" +
   * "<edge id=\"2100\" fromId=\"2000\" toId=\"2001\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"2101\" fromId=\"2000\" toId=\"2002\" type=\"2\" weight=\"2\"/>" +
   * "<edge id=\"2102\" fromId=\"2000\" toId=\"2003\" type=\"1\" weight=\"1\"/>" +
   * "<edge id=\"2103\" fromId=\"2000\" toId=\"2004\" type=\"3\" weight=\"1\"/>" +
   * "<edge id=\"9999\" fromId=\"1009\" toId=\"2000\" type=\"1\" weight=\"3\"/>" + "</root>";
   */

  static {
    relXmls.put("67ec8d4cad050b7d86974762227403c7", simpleRelXml);
    // xmls.put("1001", threeDescendentsXml);
    // relXmls.put("1009", twoRelationXml);
  }

  static {
    nodeXmls.put("程学旗", simpleNodeXml);
    nodeXmls.put("NOEXISTS", noSuchNodeXml);
  }

  static {
    mapXmls.put("MAPXML", simpleMapXml);
    mapXmls.put("NOEXISTS", noSuchMapXml);
  }

  /**
   * @return the xmls
   */
  public static Map<String, String> getRelXmls() {
    return relXmls;
  }

  /**
   * @return the nodeXmls
   */
  public static Map<String, String> getNodeXmls() {
    return nodeXmls;
  }

  /**
   * @return the mapXmls
   */
  public static Map<String, String> getMapXmls() {
    return mapXmls;
  }

}
