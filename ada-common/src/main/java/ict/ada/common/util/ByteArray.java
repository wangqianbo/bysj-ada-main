package ict.ada.common.util;

import java.util.Arrays;

/**
 * Wrapper for byte[] to use byte[] as key in HashMap.<br>
 * Copied from Hadoop.
 */
public class ByteArray {
  private int hash = 0; // cache the hash code
  private final byte[] bytes;

  public ByteArray(byte[] bytes) {
    this.bytes = bytes;
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = Arrays.hashCode(bytes);
    }
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if(o == null) return false;
    if (!(o instanceof ByteArray)) {
      return false;
    }
    return Arrays.equals(bytes, ((ByteArray) o).bytes);
  }

  @Override
  public String toString() {
    return Hex.encodeHex(bytes);
  }
}