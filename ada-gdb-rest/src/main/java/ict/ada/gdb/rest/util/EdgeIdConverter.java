package ict.ada.gdb.rest.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import ict.ada.common.model.Edge;

public class EdgeIdConverter {
  public static byte[] checkAndtoBytes(String edgeId) {
    byte[] s = null;
    try {
      s = NodeIdConveter.toBytes(edgeId);
    } catch (Exception e) {
      throw new IllegalArgumentException("the format of edgeId is wrong");
    }
    try {
      Edge.checkEdgeId(s);
    } catch (Exception e) {
      throw new IllegalArgumentException("the format of edgeId is wrong");
    }
    return s;
  }

  public static String generateErrorCodeJson(String errorCode) { // TODO move
    // this
    // method to
    // another
    // util
    // class
    Map<String, String> errorCodeMap = new HashMap<String, String>();
    if (errorCode.isEmpty()) errorCode = "errorCode";
    errorCodeMap.put("errorCode", errorCode);
    String errorCodeJson = null;

    errorCodeJson = PojoMapper.toJson(errorCodeMap, true); // assert no
    // exception
    // happens

    return errorCodeJson;
  }
}
