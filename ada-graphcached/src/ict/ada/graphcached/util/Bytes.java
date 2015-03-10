/**
 * 
 */
package ict.ada.graphcached.util;

/**
 * @author forhappy
 *
 */
public class Bytes {
  
  public static byte [] add(final byte [] a, final byte [] b) {
    byte [] result = new byte[a.length + b.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    return result;
  }
  
  public static byte [] add(final byte [] a, final byte [] b, final byte [] c) {
    byte [] result = new byte[a.length + b.length + c.length];
    System.arraycopy(a, 0, result, 0, a.length);
    System.arraycopy(b, 0, result, a.length, b.length);
    System.arraycopy(c, 0, result, a.length + b.length, c.length);
    return result;
  }
}
