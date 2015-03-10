package ict.ada.gdb.rest.util;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import ict.ada.common.model.Node;

public class NodeIdConveter {
  private static char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b',
      'c', 'd', 'e', 'f' };

  static public String toString(byte[] byteId) {
    String s = null;
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
    return StringUtils.hexStringToByte(stringId);
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

  public static void main(String[] args) {
    // byte[]a=Bytes.toBytesBinary("\\xC8\\x04");
    byte[] a = StringUtils.hexStringToByte("c804");
    // System.out.println( NodeIdConveter.toString(a));
    System.out.println(Bytes.toInt(a));
  }
}