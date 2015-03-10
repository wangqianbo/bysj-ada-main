package ict.ada.graphcached.util;


/**
 * Tool for Hex encoding.
 * 
 */
public class Hex {
  /**
   * Used to build output as Hex
   */
  private static final char[] DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
      'b', 'c', 'd', 'e', 'f' };

  /**
   * decode a hex String into a byte array.
   * 
   * @param hexStr
   * @return
   */
  public static byte[] decodeHex(String hexStr) {
    if (hexStr == null || hexStr.length() % 2 != 0)
      throw new IllegalArgumentException("hexStr=" + hexStr);

    byte[] result = new byte[hexStr.length() / 2];
    for (int i = 0; i < hexStr.length(); i += 2) {
      int f = toDigit(hexStr, i) << 4;
      f = f | toDigit(hexStr, i + 1);
      result[i / 2] = (byte) (f & 0xff);
    }
    return result;
  }

  private static int toDigit(String hexStr, int i) {
    int digit = Character.digit(hexStr.charAt(i), 16);// 16
    if (digit < 0) {
      throw new RuntimeException("Illegal hexadecimal charcter " + hexStr.charAt(i) + " at index "
          + i);
    }
    return digit;
  }

  /**
   * Encode a byte array into a Hex String
   * 
   * @param bytes
   * @return
   */
  public static String encodeHex(byte[] bytes) {
    if (bytes == null) throw new NullPointerException("null bytes");
    StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (int i = 0; i < bytes.length; i++) {
      byte b = bytes[i];
      sb.append(DIGITS[(b & 0xf0) >>> 4]);
      sb.append(DIGITS[b & 0x0f]);
    }
    return sb.toString();
  }

  public static void main(String[] args) {
    System.out.println((Hex.encodeHex(Hex.decodeHex("f180"))));
  }
}