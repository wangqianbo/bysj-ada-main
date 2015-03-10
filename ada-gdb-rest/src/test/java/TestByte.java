import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.util.StringUtils;

public class TestByte {
  public static void main(String[] args) throws UnsupportedEncodingException {
    String id = "153507c801286e1317e0dd1a70cbb48c763901358950a2469278bacc690fa71f82446c4e";

    ByteArray a = new ByteArray(StringUtils.hexStringToByte(id));
    for (byte ascii : a.getBytes())
      System.out.print((char) ascii);
    String asc = new String(a.getBytes(), "ASCII");
    // System.out.println(asc);
  }
}
