package ict.ada.graphcached.util;

import java.security.NoSuchAlgorithmException;

public class MD5Digest {
  /**
   * Calculate md5 digest. 
   * @param source The source bytes to calculate.
   * @return 16-hex-char representation of the source's md5 digest. 
   */
  public static String MD5Hex(byte[] source) {
    String s = null;
    char hexDigits[] =
        {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    try {
      java.security.MessageDigest md = java.security.MessageDigest.getInstance("MD5");
      md.update(source);
      byte tmp[] = md.digest();
      char str[] = new char[16 * 2];
      int k = 0;
      for (int i = 0; i < 16; i++) {
        byte byte0 = tmp[i];
        str[k++] = hexDigits[byte0 >>> 4 & 0xf];
        str[k++] = hexDigits[byte0 & 0xf];
      }
      s = new String(str);
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    return s;
  }
  
  /**
   * Calculate md5 digest. 
   * @param source The source string to calculate.
   * @return 16-hex-char representation of the source's md5 digest. 
   */
  public static String MD5Hex(String source) {
    return MD5Hex(source.getBytes());
  }
  
  public static byte[] MD5(byte[] source) {
    java.security.MessageDigest md;
    try {
      md = java.security.MessageDigest.getInstance("MD5");
      md.update(source);
      byte tmp[] = md.digest();
      return tmp;
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    return null;
  }
  
  public static byte[] MD5(String source) {
    return MD5(source.getBytes());
  }
}
