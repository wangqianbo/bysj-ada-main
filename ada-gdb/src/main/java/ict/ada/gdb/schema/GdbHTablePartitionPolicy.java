package ict.ada.gdb.schema;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.AdaModeConfig;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;


/**
 * Control how we store different types of Node and Edge into different HBase tables.<br>
 * Control HBase table name format
 * 
 */
public class GdbHTablePartitionPolicy {
  public static String getNodeRelatedHTableName(GdbHTableType tableType, Channel type) {
    switch (tableType) {
    case NODE_NAME:
    case NODE_ATTR:
    case NODE_WDEREFS:
    case NODE_ID:
    case NODE_TASK:	
      return GdbHTableConstant.SHARED_PREFIX + "-" + tableType.getContentTag() + "-"
          + getPartitionName(type)+"-"+getDBVersionName(type);

    case EDGE_ID:
    case EDGE_REL_WEIGHT_DETAIL:
    case EDGE_REL_WEIGHT_SUM:
    case RELATION_WDEREFS:
    case EDGE_MEM:
      throw new RuntimeException(
          "BUGS in code. Edge related Tables should not call getNodeRelatedHTableName()");
    default:
      throw new IllegalArgumentException("tableType=" + tableType
          + ". Perhaps a new Table type is added in " + GdbHTableType.class.getCanonicalName()
          + " ,but not added in " + GdbHTablePartitionPolicy.class.getCanonicalName());
    }
  }

  /**
   * Get Edge related HTable names without supporting aggregate types.
   * 
   * @param tableType
   * @param headType
   *          can not be aggregate types
   * @param tailType
   *          can not be aggregate types
   * @return
   */
  public static String getEdgeRelatedHTableNameWithoutAggType(GdbHTableType tableType,
      Channel type) {
    switch (tableType) {
    case NODE_NAME:
    case NODE_ATTR:
    case NODE_WDEREFS:
    case NODE_ID:
    case NODE_TASK:	
      throw new RuntimeException(
          "BUGS in code. Node related Tables should not call getEdgeRelatedHTableName()");

    case EDGE_ID:
    case EDGE_REL_WEIGHT_DETAIL:
    case EDGE_REL_WEIGHT_SUM:
    case RELATION_WDEREFS:
    case EDGE_MEM:
      return GdbHTableConstant.SHARED_PREFIX + "-" + tableType.getContentTag() + "-"
          + getPartitionName(type)+"-"+getDBVersionName(type);
    case RELATION_TYPE:
//    	return GdbHTableConstant.SHARED_PREFIX + "-" + tableType.getContentTag() + "-"
//        + getPartitionName(type)+"-"+getDBVersionName(type);
      return GdbHTableConstant.SHARED_PREFIX + "-" + tableType.getContentTag() + "-"+getDBVersionName(type);
    default:
      throw new IllegalArgumentException("tableType=" + tableType
          + ". Perhaps a new Table type is added in " + GdbHTableType.class.getCanonicalName()
          + " ,but not added in " + GdbHTablePartitionPolicy.class.getCanonicalName());
    }
  }

 
  /**
   * Get the correspondent HTable partition name of the given NodeType.
   * <p>
   * type is the  "hint" for determining partition names. <br>
   * <p>
   * 
   * @param type
   *          can not be null nor aggregate type
   * @return
   */
  private static String getPartitionName(Channel type) {
    if (type == null) throw new NullPointerException("null type");
    // type2 can be null, type1 can not.
    // type1 and type2 can not be AggregateType.
    if (type==Channel.ANY )
      throw new IllegalArgumentException("type=" + type 
          + ". Aggregate Channel should never appear here, please check code for BUGS.");

    switch (type) {
    // Non-shared types : various Person types
    case WEB:
      return "web";
    case NEWS:
    	return "news";
    case BLOG:
    	return "blog";
    case SCHOLAR:
      return "scholar";

    case BBS:
      return "bbs";

    case WEIBO:
      return "weibo";

    case TWITTER:
      return "twitter";

    case LINKEDIN:
      return "linkedin";

    case BAIDUBAIKE:
    	return "baidubaike";
    case HUDONGBAIKE:
    	return "hudongbaike";
    case WIKIBAIKE:
      return "wikibaike";
    case DATA973:
    	return "data973";
    case KNOWLEDGE:
      return "knowledge";
    case DATA:
    	return "proj_data";
    case COMPUTE:
    	return "compute";
    case MERGE:
    	return "merge";
    case EL:
    	return "el";
    case ER:
    	return "er";
    default:
      throw new IllegalArgumentException("type=" + type + ". Perhaps a new type is added in "
          + NodeType.class.getCanonicalName() + ", but is not added in "
          + GdbHTablePartitionPolicy.class.getCanonicalName());
    }
  }

  private static String getDBVersionName(Channel type){
	  if (type == null) throw new NullPointerException("null type");
	    // type2 can be null, type1 can not.
	    // type1 and type2 can not be AggregateType.
	    if (type==Channel.ANY )
	      throw new IllegalArgumentException("type=" + type 
	          + ". Aggregate Channel should never appear here, please check code for BUGS.");
        return AdaModeConfig.getDBVersion(type);
  }
  
 // private static final String SHARED_PARTITION_NAME = "shared";


  /**
   * Generate HTable names. Used in ict.ada.gdb.schema.util.ScriptsGenerator
   * 
   * @param requiredNodeTypes
   *          if null, will return all HTable names. Otherwise, will only return tables for the
   *          given NodeTypes' partitions.
   * @return
   */
  public static Set<String> generateHTableNames(Channel[] requiredChannels) {
    LinkedHashSet<String> nameList = new LinkedHashSet<String>();
    Set<Channel> requiredChannelSet = requiredChannels != null ? new HashSet<Channel>(
        Arrays.asList(requiredChannels)) : null;
        if(requiredChannelSet.contains(Channel.ANY))
        	for(Channel channel:Channel.values())
        	requiredChannelSet.add(channel);
    for (GdbHTableType tableType : GdbHTableType.values()) {
      for (Channel channel : Channel.values()) {
        if (channel!=Channel.ANY
            && (requiredChannelSet == null || requiredChannelSet.contains(channel))) {
          switch (tableType) {
          case NODE_NAME:
          case NODE_ATTR:
          case NODE_WDEREFS:
          case NODE_ID:
          case NODE_TASK:	
            nameList.add(GdbHTablePartitionPolicy.getNodeRelatedHTableName(tableType, channel));
            break;

          case EDGE_ID:
          case EDGE_REL_WEIGHT_DETAIL:
          case EDGE_REL_WEIGHT_SUM:
          case RELATION_WDEREFS:
          case EDGE_MEM:
            nameList.add(GdbHTablePartitionPolicy.getEdgeRelatedHTableNameWithoutAggType(tableType, channel));
            break;
          case LOCATION_NODE_TASKS:
          
            if (requiredChannels == null) nameList.add(LocationNodeTasksHTable.getName());
            break;
          case RELATION_TYPE:
        	  break;
          default:
            throw new RuntimeException("Unknown table type=" + tableType);
          }
        }
      }
    }
    return nameList;
  }
  public static void main(String[] args){
	  for(String tableName:GdbHTablePartitionPolicy.generateHTableNames(new Channel[]{Channel.WEB}))
		  System.out.println(tableName);;
  }
}
