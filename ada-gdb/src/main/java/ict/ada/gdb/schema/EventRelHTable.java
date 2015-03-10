package ict.ada.gdb.schema;


public class EventRelHTable {
  // Schema
  public static final byte[] FAMILY = "i".getBytes();
  private static final String tableName="ada_event_rel";

  /**
   * 
   * @return
   */
  public static String getName() {
    return tableName;
  }

}
