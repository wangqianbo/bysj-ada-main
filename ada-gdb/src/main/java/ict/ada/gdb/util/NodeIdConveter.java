package ict.ada.gdb.util;

import ict.ada.common.model.Node;

public class NodeIdConveter {
  static public String toString(byte[] byteId) {
    String s = null;
    char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e',
        'f' };
    char str[] = new char[byteId.length * 2];
    int k = 0;
    for (int i = 0; i < byteId.length; i++) {
      byte byte0 = byteId[i];
      str[k++] = hexDigits[byte0 >>> 4 & 0xf];
      str[k++] = hexDigits[byte0 & 0xf];
    }
    s = new String(str);
    return s;
  }

  static public byte[] toBytes(String stringId) {
    int idLength = stringId.length();
    if (idLength % 2 != 0) return null;
    byte[] s = new byte[idLength / 2];
    int k = 0;
    for (int i = 0; i < idLength; i += 2) {
      char high = stringId.charAt(i);
      char low = stringId.charAt(i + 1);
      int oneInt = ((Integer.valueOf(String.valueOf(high), 16) << 4))
          | (Integer.valueOf(String.valueOf(low), 16));
      s[k++] = (byte) oneInt;
    }
    return s;
  }

  public static byte[] checkAndtoBytes(String nodeId) {
    byte[] s = null;
    try {
      s = toBytes(nodeId);
    } catch (Exception e) {
      throw new IllegalArgumentException("the format of nodeId is wrong");
    }
    try {
      Node.checkNodeId(s);
    } catch (Exception e) {
      throw new IllegalArgumentException("the format of nodeId is wrong");
    }
    return s;
  }

}