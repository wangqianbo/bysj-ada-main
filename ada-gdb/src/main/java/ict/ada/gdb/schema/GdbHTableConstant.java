package ict.ada.gdb.schema;

import ict.ada.gdb.common.AdaConfig;


/**
 * Constants for HBase tables used in GDB.
 */
public class GdbHTableConstant {

  /** The shared prefix of all GDB tables in HBase */
  public static final String SHARED_PREFIX = "gdb";

  /** The minimal time granularity for a time range. */
  public static final int TIME_GRANULARITY = AdaConfig.TIME_GRANULARITY;// Set to one hour

  public static final int ACTION_TIME_GRANULARITY = 24*3600;// Set to 24 hours

}
