package ict.ada.gdb.schema.util;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.schema.GdbHTablePartitionPolicy;

import java.util.Set;

/**
 * Generator for HTable name related scripts.
 * 
 */
public class ScriptsGenerator {
  /**
   * Generate table names in HBase
   */
  private static void generateCreateTableScripts(Channel... requiredPartition) {
   fillTemplateWithTableNames("create '%s', { NAME => 'i',%s COMPRESSION => 'SNAPPY' }",
        requiredPartition);
	  //fillTemplateWithTableNames("create '%s', { NAME => 'i' }",
		      //  requiredPartition);
  }

  private static void fillTemplateWithTableNames(String template, Channel[] requiredPartition) {
    Set<String> nameSet = GdbHTablePartitionPolicy.generateHTableNames(requiredPartition);
    for (String tableName : nameSet) {
    	String bloomfilter="";
    	if(tableName.contains("nodeId")||tableName.contains("edgeId"))
    		bloomfilter="BLOOMFILTER => 'ROW' ,";
      System.out.printf(template + "\n", tableName,bloomfilter);
    }
    System.out.println("\nTotal lines: " + nameSet.size());
  }

  private static void generateDisableTableScript(Channel[] requiredPartition) {
    fillTemplateWithTableNames("disable '%s' ", requiredPartition);
  }

  private static void generateDropTableScript(Channel[] requiredPartition) {
    fillTemplateWithTableNames("drop '%s' ", requiredPartition);
  }

  private static void generateTruncateTableScript(Channel[] requiredPartition) {
    fillTemplateWithTableNames("truncate '%s' ", requiredPartition);
  }

  public static void main(String[] args) {
	  //AdaModeConfig.setMode(AdaModeConfig.GDBMode.INSERT);
    /*
     * Create
     */
        
    generateCreateTableScripts(Channel.KNOWLEDGE);//create all tables
    //
    // generateCreateTableScripts(new NodeType[] { NodeType.WEB_PERSON, NodeType.BAIDU_BAIKE_PERSON,
    // NodeType.CATEGORY, NodeType.IM_ATTR });

    // generateCreateTableScripts(new NodeType[] {
    // NodeType.CATEGORY, NodeType.IM_ATTR });

    /*
     * Truncate
     */

    // generateTruncateTableScript(new NodeType[] { NodeType.WEB_PERSON });

    // generateTruncateTableScript(null);// truncate all tables

    /*
     * Disable and Drop
     */
    //generateDisableTableScript(Channel.values());
    //generateDropTableScript(Channel.values());

    /*
     * temp
     */
    // fillTemplateWithTableNames("disable '%s'",
    // new NodeType[] { NodeType.CATEGORY, NodeType.IM_ATTR });
    //
    // fillTemplateWithTableNames("drop '%s'", new NodeType[] { NodeType.CATEGORY, NodeType.IM_ATTR
    // });

  }
}
