package ict.ada.gdb.schema;


public class EventDocRelHTable {
  // Schema
  public static final byte[] FAMILY = "i".getBytes();
  private static final String tableName="ada_event_doc_rel";

  /**
   * @return
   */
  public static String getName() {
    return tableName;
  }

}
