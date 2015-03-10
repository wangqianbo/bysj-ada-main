package ict.ada.gdb.model.util;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.DeserializationConfig;

public class PojoMapper {

  private static ObjectMapper m = new ObjectMapper();
  private static JsonFactory jf = new JsonFactory();
  static {
    m.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static <T> Object fromJson(String jsonAsString, Class<T> pojoClass)
      throws JsonMappingException, JsonParseException, IOException {
    return m.readValue(jsonAsString, pojoClass);
  }

  public static <T> Object fromJson(Object jsonOb, Class<T> pojoClass) throws JsonMappingException,
      JsonParseException, IOException {
    return m.convertValue(jsonOb, pojoClass);
  }

  public static <T> Object fromJson(FileReader fr, Class<T> pojoClass) throws JsonParseException,
      IOException {
    return m.readValue(fr, pojoClass);
  }

  public static String toJson(Object pojo, boolean prettyPrint) {
    StringWriter sw = new StringWriter();
    JsonGenerator jg;
    try {
      jg = jf.createJsonGenerator(sw);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (prettyPrint) {
     jg.useDefaultPrettyPrinter();
    }
    try {
      m.writeValue(jg, pojo);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return sw.toString();
  }

  public static void toJson(Object pojo, FileWriter fw, boolean prettyPrint)
      throws JsonMappingException, JsonGenerationException, IOException {
    JsonGenerator jg = jf.createJsonGenerator(fw);
    if (prettyPrint) {
      jg.useDefaultPrettyPrinter();
    }
    m.writeValue(jg, pojo);
  }
}
