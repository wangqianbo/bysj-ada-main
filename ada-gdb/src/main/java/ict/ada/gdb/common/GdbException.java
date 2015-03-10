package ict.ada.gdb.common;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;

/**
 * Base class for any Exception in Ada system.
 */
public class GdbException extends Exception {
  private static final long serialVersionUID = -536108207161571909L;
  private static Logger logger = Logger.getLogger(GdbException.class);  
  /**
   * Error code definition
   */
  // TODO
  public static final int ILLEGAL_NODE_TYPE = -1;
  public static final int ILLEGAL_EDGE_TYPE = -2;
  public static final int ILLEGAL_DETAIL = -3;
  public static final int DATA_NOT_EXIST = -4;
  public static final int INTERNAL_ERROR = -5;
  // public static final int HBASE_IO_ERROR = -6;

  private int errorCode;

  public GdbException(int errorCode) {
    this.errorCode = errorCode;
    logger.info(getstactTraceString());
  }

  public GdbException(Throwable cause) {
    super(cause);
    this.errorCode = INTERNAL_ERROR;
    logger.info(getstactTraceString());
  }

  public GdbException(String msg, Throwable cause) {
    super(msg, cause);
    this.errorCode = INTERNAL_ERROR;
    logger.info(getstactTraceString());
  }

  public GdbException(String msg) {
    super(msg);
    this.errorCode = INTERNAL_ERROR;
    logger.info(getstactTraceString());
  }
private String getstactTraceString(){
	 StringWriter trace =new StringWriter();
	    this.printStackTrace(new PrintWriter(trace));
	    return trace.toString();
}
  public int getErrorCode() {
    return errorCode;
  }

}
