package ict.ada.search;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.common.util.Triplet;
import ict.ada.search.AdaSearchMQService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import cn.golaxy.yqpt2.dtsearch2.client.DTSearchClient;

/**
 * Bean for Json generation 应该是定制好的.
 */
class IndexContent {
  private String sname;
  private String name;
  private String addl;
  int type;


  public String getSname() {
    return sname;
  }

  public void setSname(String sname) {
    this.sname = sname;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
  @JsonProperty("addl")
  public String getAddl() {
    return addl;
  }

  @JsonProperty("addl")
  public void setAddl(String addl) {
    this.addl = addl;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

}



public class AdaSearchService {
  private ObjectMapper objectMapper = null;
  private AdaSearchMQService mqClient = null;

  /**
   * This constructor use AdaSearchMQService as a client to delegate all the operations to
   * DTSearch2.
   * 
   * @param indexServerAddr e.g. 10.61.1.21:9000
   * @param mqServerAddr e.g. 10.61.1.21:6379
   */
  public AdaSearchService(String indexServerAddr, String mqServerAddr) {
    this(indexServerAddr, mqServerAddr, "ada_search_mq");
  }

  public AdaSearchService(String indexServerAddr, String mqServerAddr, String mqName) {
    this.mqClient = new AdaSearchMQService(indexServerAddr, mqServerAddr, mqName);
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Get a read-only copy of the internal type Map.
   * 
   * @return
   */

  /**
   * Delete data of the given type's search index channel
   * 
   * @param type
   * @return
   */
  public boolean deleteIndex(short indexType) { // indexType has version, like 1053
    return mqClient.deleteIndex(indexType);
  }


  public boolean addIndex(String name, String value, String additional, NodeType type,
      short indexType) {
    String jsonString;

    IndexContent indexContent = new IndexContent();
    indexContent.setSname(name);
    indexContent.setName(value);
    indexContent.setAddl(additional);// 如果additional==null则additional不在json中.
    indexContent.setType(type.getIntegerForm());
    try {
      jsonString = objectMapper.writeValueAsString(indexContent);
      jsonString = "[" + jsonString + "]";
    } catch (IOException e) {
      throw new RuntimeException("Json generation failed. name=" + name + " value=" + value
          + " type=" + type);
    }
    // pass it to MQ
    if (!mqClient.index(indexType, jsonString)) {
      System.out.println("\033[31m Index operation failed to commit to MQ\033[39m");
      return false;
    } else {
      return true;
    }
  }

  /**
   * Add node name to index system.
   * 
   * @param rawData data to be indexed, must be in the same channel, rawData has the following
   *        format: [{key1:value1}, {key2:value2}, ...].
   * @return true if all data indexed successfully, otherwise return false.
   */
  public boolean addIndexBatched(List<Triplet<String, String, String>> rawData, NodeType type,
      short indexType) {
    String indexedJson;
    List<IndexContent> indexContents = new ArrayList<IndexContent>();
    for (Triplet<String, String, String> entry : rawData) {
      IndexContent indexContent = new IndexContent();
      indexContent.setSname(entry.getFirst());
      indexContent.setName(entry.getSecond());
      indexContent.setAddl(entry.getThird());
      indexContent.setType(type.getIntegerForm());// 这个type应该是点的type,不知会不会有问题.
      indexContents.add(indexContent);
    }

    try {
      indexedJson = objectMapper.writeValueAsString(indexContents);
    } catch (IOException e) {
      throw new RuntimeException("Json generation failed.");
    }

    // pass it to MQ
    if (!mqClient.index(indexType, indexedJson)) {
      System.out.println("\033[31m Index operation failed to commit to MQ\033[39m");
      return false;
    } else {
      return true;
    }
  }

  // just for compatibility
  public boolean addIndexBatched2(List<Triplet<String, String, String>> rawData, NodeType type,
      short indexType) {
    return addIndexBatched(rawData, type, indexType);
  }

  public static void testMQ() throws InterruptedException {
    AdaSearchService s = new AdaSearchService("10.61.1.21:9000", "10.61.1.21:6379");
    // s.deleteIndex((short) Channel.WEB.getIntForm());
    // Thread.sleep(10000);
    for (int i = 10; i < 20; ++i) {
      NodeType nodeType = NodeType.getType(Channel.WEB, Attribute.PERSON);
      s.addIndex("倪光南" + i, "倪光南" + i, "计算所", nodeType, (short) nodeType.getChannel().getIntForm());
      Thread.sleep(50);
    }
    // Thread.sleep(2000);
    s.mqClient.search("倪光南", Channel.WEB, Attribute.PERSON);
  }

  public static void main(String[] args) throws InterruptedException {
    
	  DTSearchClient client1 = new DTSearchClient();
	  client1.SetDTService("10.100.1.35:9000");
	  
	  System.out.println(client1.ClearChannel((short)200));
    /*
     * IndexContent ic1 = new IndexContent(); ic1.setSname("qqqq"); ic1.setName("ic1value"); //
     * ic1.setAddl("addl1"); ic1.setType(53001);
     * 
     * IndexContent ic2 = new IndexContent(); ic2.setSname("qqqq"); ic2.setName("ic2-value"); //
     * ic2.setAddl("addl2"); ic2.setType(200001);
     * 
     * IndexContent ic3 = new IndexContent(); ic3.setSname("IndexContent");
     * ic3.setName("ic3-value"); ic3.setType(1);
     * 
     * List<IndexContent> icList = new ArrayList<IndexContent>(); icList.add(ic1); icList.add(ic2);
     * // icList.add(ic3);
     * 
     * ObjectMapper objectMapper = new ObjectMapper(); DTSearchClient client1 = new
     * DTSearchClient(); // -- For DTSearch2-1.0, SetDTService(master, multi_index) doesn't exist --
     * // client1.SetDTService("10.61.1.21:9000","10.61.1.21:8041");
     * client1.SetDTService("10.61.1.21:9000"); //
     * System.out.println(TypeMapper.ConvertToIndexType(NodeType.BAIDU_BAIKE_PERSON)); try { String
     * x = objectMapper.writeValueAsString(icList); //
     * System.out.println(client1.ClearChannel((short)53)); //
     * System.out.println(client1.Index((short)53, x)); System.out.println(x);
     * 
     * String query = "[FIELD]( type, [FILTER]( [FIELD]( sname, \"" + "李开复" + "\" ), ==, " + 8001 +
     * " ) )"; System.out.println(query); DTSearchResult result = new DTSearchResult(); // Search
     * Result List<String> fields = new LinkedList<String>(); fields.add("name");
     * fields.add("sname"); fields.add("type"); fields.add("addl"); boolean reverse = false;
     * client1.Search((short) 53, query, fields, 0, 1000, reverse, result);
     * System.out.println(result.ret_code); System.out.println(result.matchs);
     * System.out.println(result.docs.get(0).fields.get(0)); } catch (JsonGenerationException e) {
     * e.printStackTrace(); } catch (JsonMappingException e) { e.printStackTrace(); } catch
     * (IOException e) { e.printStackTrace(); }
     */
  }
}
