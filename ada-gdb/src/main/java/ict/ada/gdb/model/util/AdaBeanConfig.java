package ict.ada.gdb.model.util;

import ict.ada.common.model.Node;
import ict.ada.gdb.dataloader.GdbDataLoaderException;
import ict.ada.gdb.dataloader.NodeMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class AdaBeanConfig {
  private static final String ADA_BEAN_CONFIG_PATH ="src/main/resources";
  private static Map<String, Object> beanConfigTemplate = new HashMap<String, Object>();
  private static ObjectMapper jacksonMapper = new ObjectMapper();

  static {
    CharBuffer buffer = CharBuffer.allocate(10 * 1024);
    File dir = new File(ADA_BEAN_CONFIG_PATH);
    File[] files = dir.listFiles();
    for (File configFile : files) {
      String config = "";
      String name = configFile.getName();
      String[] split = name.split("_");
      if(!name.endsWith("GDB"))
        continue;
      BufferedReader configReader = null;
      try {
        configReader = new BufferedReader(new FileReader(configFile));
        Class<?> beanClass = Class.forName("ict.ada.gdb.model.util."+split[0], false, AdaBeanConfig.class.getClassLoader());
        buffer.clear();
        while (true) {
          configReader.read(buffer);
          if (buffer.hasRemaining()) {
            buffer.flip();
            config += buffer.toString();
            break;
          } else {
            buffer.flip();
            config += buffer.toString();
          }
          buffer.clear();
        }
        //System.out.println(config);
        Object configBean = jacksonMapper.readValue(config, beanClass);
        beanConfigTemplate.put(name, configBean);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      } finally {
        try {
          if (configReader != null) configReader.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }
  public static void main(String[] args) throws JsonMappingException, JsonParseException, IOException, GdbDataLoaderException{
    NodeBean bean = (NodeBean) beanConfigTemplate.get("NodeBean_Test_GDB");
    //System.out.println(bean.sName);
    NodeBeanTest test = new NodeBeanTest();
    NodeBean nodeBean = bean.construct(NodeBeanTest.class, test);
    String json = PojoMapper.toJson(nodeBean, false);
    System.out.println(json);
    Node node  = NodeMapper.toNode(json);
    System.out.println(node.getSnames());
    
  }
}
