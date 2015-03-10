package ict.ada.gdb.schema;

public class LocationNodeTasksHTable {
  public static final String FAMILY = "i";// column family
  public static final String QUALIFIER = "a";// qualifier: node attribute("a")

  /**
   * Table name generation
   * 
   * @param type
   * @return
   */
  public static String getName() {
    return GdbHTableConstant.SHARED_PREFIX + "-"
        + GdbHTableType.LOCATION_NODE_TASKS.getContentTag();
  }

}
