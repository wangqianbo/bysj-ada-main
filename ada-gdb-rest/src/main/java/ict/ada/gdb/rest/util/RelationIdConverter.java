package ict.ada.gdb.rest.util;

import ict.ada.common.model.Relation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

public class RelationIdConverter {
  public static byte[] checkAndtoBytes(String relationId) {
    byte[] s = null;
    try {
      s = NodeIdConveter.toBytes(relationId);
    } catch (Exception e) {
      throw new IllegalArgumentException("the format of relationId is wrong");
    }
    try {
      Relation.checkRelationId(s);
    } catch (Exception e) {
      throw new IllegalArgumentException("the format of relationId is wrong");
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
    return errorCodeJson;
  }
}
