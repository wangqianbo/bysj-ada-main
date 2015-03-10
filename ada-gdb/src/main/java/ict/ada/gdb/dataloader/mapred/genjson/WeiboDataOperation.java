package ict.ada.gdb.dataloader.mapred.genjson;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;

import java.util.HashMap;
import java.util.Map;

public class WeiboDataOperation {

	  private DataOpType type;
	  private Object data;

	  public WeiboDataOperation(DataOpType type, Object data) {
	    if (type == null || data == null) {
	      throw new NullPointerException("type=" + type + " data=" + data);
	    }
	    if (data.getClass() != type.getDataClass()) {
	      throw new IllegalStateException("Bug. data Class=" + data.getClass());
	    }
	    this.type = type;
	    this.data = data;
	  }

	  /**
	   * Get the attached data.<br>
	   * If DataOpType=ADD_NODE, data should be a Node object.<br>
	   * If DataOpType=ADD_EDGE, data should be an Edge object.
	   */
	  public <T> T getData() {
	    T result = (T) data;// If this conversion fails, BUG exists .
	    return result;
	  }

	  public DataOpType getType() {
	    return type;
	  }

	  /**
	   * Data Operation Types
	   */
	  public static enum DataOpType {
	    ADD_NODE('1', Node.class), ADD_EDGE('2', Edge.class), ;

	    private char typeCode;
	    private Class<?> dataClass;

	    private DataOpType(char typeCode, Class<?> dataClass) {
	      this.typeCode = typeCode;
	      this.dataClass = dataClass;
	    }

	    public Class<?> getDataClass() {
	      return dataClass;
	    }

	    private static Map<Character, DataOpType> lookup;
	    static {
	      lookup = new HashMap<Character, WeiboDataOperation.DataOpType>();
	      for (DataOpType dot : DataOpType.values()) {
	        lookup.put(dot.typeCode, dot);
	      }
	    }

	    /**
	     * Get DataOpType from char.
	     * 
	     * @param typeCode
	     * @return null if this is not a valid char
	     */
	    public static DataOpType fromChar(char typeCode) {
	      return lookup.get(typeCode);
	    }

	  }

}
