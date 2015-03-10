package ict.ada.common.model;

import ict.ada.common.deployment.AdaCommonConfig;
import ict.ada.common.util.Hex;

/**
 * 
 * A reference to data in WDE
 * 
 */
public class WdeRef {
  public static final int WDEID_SIZE = AdaCommonConfig.DEPLOYMENTSPEC.getWdeRefIdLen();// WDE id is a long, so 8 bytes. TODO wdeid format

  private int timestamp;// unix_timestamp in seconds

  private byte[] wdeId;
  private int offset;
  private int length;

  public WdeRef(byte[] wdeId, int offset, int length) {
    this(wdeId, offset, length, 0);// Default timestamp is 0
  }

  public WdeRef(byte[] wdeId, int offset, int length, int timestamp) {
    if (wdeId == null) throw new NullPointerException("null wdeId");
    if (wdeId.length != WDEID_SIZE)
      throw new IllegalArgumentException("wdeId size =" + wdeId.length);
    if (offset < 0 || length < 0 || (offset > 0 && length == 0))
      throw new IllegalArgumentException("offset=" + offset + " length=" + length);
    if (timestamp < 0) throw new IllegalArgumentException("timestamp=" + timestamp);
    this.wdeId = wdeId;
    this.offset = offset;
    this.length = length;
    this.timestamp = timestamp;
  }

  public void setTimestamp(int ts) {
    if (ts < 0) throw new IllegalArgumentException("timestamp=" + timestamp);
    this.timestamp = ts;
  }

  /**
   * @return the timestamp
   */
  public int getTimestamp() {
    return timestamp;
  }

  /**
   * @return the wdeId 
   */
  public byte[] getWdeId() {
    return wdeId;
  }

  /**
   * @return the offset
   */
  public int getOffset() {
    return offset;
  }

  /**
   * @return the length
   */
  public int getLength() {
    return length;
  }
  @Override
  public String toString() {
    return "id=" + Hex.encodeHex(wdeId) + " len=" + length + " off=" + offset + " ts=" + timestamp;
  }

}
