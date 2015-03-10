package ict.ada.common.util;

/**
 * Utility class that handles byte arrays. Copied from org.apache.hadoop.hbase.util.Bytes in HBase.
 * 
 */
public class BytesTool {

  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  /**
   * @param a
   *          lower half
   * @param b
   *          upper half
   * @return New array that has a in lower half and b in upper half.
   */
  public static byte[] add(final byte[] a, final byte[] b) {
    return add(a, b, EMPTY_BYTE_ARRAY);
  }

  /**
   * @param a
   *          first third
   * @param b
   *          second third
   * @param c
   *          third third
   * @return New array made from a, b and c
   */
  public static byte[] add(final byte[] a, final byte[] b, final byte[] c) {
    byte[] result = new byte[a.length + b.length + c.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    System.arraycopy(c, 0, result, a.length + b.length, c.length);
    return result;
  }

}
