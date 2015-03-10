package ict.ada.gdb.schema;


public class CommunityPersonRelHTable {

  public static final String FAMILY = "l";// column family
  public static final String QUALIFIER = "cm";// column family
  private static final String TABLENAME="ada_community_person_rel";

  /**
   * Table name generation
   * 
   * @return
   */
  public static String getName() {
    return TABLENAME;
  }

}
