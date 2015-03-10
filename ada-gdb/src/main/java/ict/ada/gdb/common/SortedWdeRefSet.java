package ict.ada.gdb.common;

import ict.ada.common.model.WdeRef;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A sorted set of WdeRefs.<br>
 * WdeRefs in this set will be sorted lexicographically according to its byte array representation.
 * 
 */
public class SortedWdeRefSet {
  private static Log LOG = LogFactory.getLog(SortedWdeRefSet.class);

  // size in byte
  private static final int WDEID_SIZE = WdeRef.WDEID_SIZE;
  private static final int INT_SIZE = 4;
  public static final int WDEREF_SIZE = WDEID_SIZE + 2 * INT_SIZE;

  private byte[] internalBytes;

  /**
   * Construct a SortedWdeRefSet with its byte array representation.
   * 
   * @param bytes
   *          if bytes is null, an empty set will be created.
   */
  public SortedWdeRefSet(byte[] bytes) {
    if (bytes != null && bytes.length % WDEREF_SIZE != 0)
      throw new IllegalArgumentException("bytes size: " + bytes.length);
    this.internalBytes = (bytes == null) ? new byte[0] : bytes;
    // a size check
    if (internalBytes.length / WDEREF_SIZE > 20000) {
      LOG.warn("Is WdeRefSet too large ? WdeRef Count=" + (internalBytes.length / WDEREF_SIZE));
    }
  }

  /**
   * Construct an empty SortedWdeRefSet.
   */
  public SortedWdeRefSet() {
    internalBytes = new byte[0];
  }

  /**
   * Construct a SortedWdeRefSet with a WdeRef List
   * 
   * @param wdeRefList
   *          if wdeRefList is null or empty, an empty set will be created
   */
  public SortedWdeRefSet(List<WdeRef> wdeRefList) {//并没有去重。我认为还是有必要去重的。
    if (wdeRefList == null || wdeRefList.size() == 0) {
      internalBytes = new byte[0];
    } else {
      List<WdeRef> sortedList = new ArrayList<WdeRef>(wdeRefList);// protective copy
      Collections.sort(sortedList, new Comparator<WdeRef>() {
        @Override
        public int compare(WdeRef o1, WdeRef o2) {
          return Bytes.compareTo(getBytesFromWdeRef(o1), getBytesFromWdeRef(o2));
        }
      });
      internalBytes = new byte[sortedList.size() * WDEREF_SIZE];
      for (int i = 0; i < sortedList.size(); i++) {
        WdeRef wdeRef = sortedList.get(i);
        int pos = i * WDEREF_SIZE;
        System.arraycopy(getBytesFromWdeRef(wdeRef), 0, internalBytes, pos, WDEREF_SIZE);
      }
    }
  }

  private byte[] getBytesFromWdeRef(WdeRef wdeRef) {
    if (wdeRef == null) throw new NullPointerException("null WdeRef");

    byte[] arr = new byte[WDEREF_SIZE];
    // A WdeRef is saved as |---wdeid---|--offset(int)--|--length(int)--|
    System.arraycopy(wdeRef.getWdeId(), 0, arr, 0, WDEID_SIZE);
    System.arraycopy(Bytes.toBytes(wdeRef.getOffset()), 0, arr, WDEID_SIZE, INT_SIZE);
    System.arraycopy(Bytes.toBytes(wdeRef.getLength()), 0, arr, WDEID_SIZE + INT_SIZE, INT_SIZE);
    return arr;
  }

  private WdeRef getWdeRefFromBytes(byte[] arr, int pos) {
    byte[] wdeId = new byte[WDEID_SIZE];
    System.arraycopy(arr, pos, wdeId, 0, WDEID_SIZE);
    int offset = Bytes.toInt(arr, pos + WDEID_SIZE, INT_SIZE);
    int length = Bytes.toInt(arr, pos + WDEID_SIZE + INT_SIZE, INT_SIZE);
    return new WdeRef(wdeId, offset, length);
  }
  private byte[] getWdeIdFromBytes(byte[] arr,int pos){
    byte[] wdeId = new byte[WDEID_SIZE];
    System.arraycopy(arr, pos, wdeId, 0, WDEID_SIZE);
    return wdeId;
  }

  /**
   * Get a List representation of WdeRefs in this Set.<br>
   * Timestamp of WdeRef in the results is set to ZERO<br>
   * This method will trigger conversion between WdeRef and its byte array representation, so avoid
   * frequent invocations.
   * 
   * @return
   */
  public List<WdeRef> getList() {
    int wdeRefCount = internalBytes.length / WDEREF_SIZE;
    if (wdeRefCount == 0) {
      return Collections.emptyList();
    } else {
      List<WdeRef> list = new ArrayList<WdeRef>(wdeRefCount);
      for (int i = 0; i < wdeRefCount; i++) {
        list.add(getWdeRefFromBytes(internalBytes, i * WDEREF_SIZE));
      }
      return list;
    }
  }
public List<byte[]> getWdeIdList(){
  int wdeRefCount = internalBytes.length / WDEREF_SIZE;
  if (wdeRefCount == 0) {
    return Collections.emptyList();
  } else {
    List<byte[]> list = new ArrayList<byte[]>(wdeRefCount);
    for (int i = 0; i < wdeRefCount; i++) {
      list.add(getWdeIdFromBytes(internalBytes, i * WDEREF_SIZE));
    }
    return list;
  }
}
  /**
   * Get the Byte array representation of this Set.<br>
   * The result can be stored into HBase directly.
   * 
   * @return
   */
  public byte[] getBytes() {
    return internalBytes;
  }

  public int size() {
    return internalBytes.length / WDEREF_SIZE;
  }

  /**
   * Add all WdeRefs in the given List to this Set.<br>
   * If the WdeRef has been in this Set, it will be ignored.
   * 
   * @param refList
   */
  public void add(List<WdeRef> refList) {
    if (refList == null) return;
    for (WdeRef ref : refList) {
      add(ref);
    }
  }

  /**
   * Add one WdeRef to this Set. If the WdeRef has been in this Set, it will be ignored.
   * 
   * @param wdeRef
   */
  public void add(WdeRef wdeRef) {//这个地方考虑了去重
    int wdeRefCount = internalBytes.length / WDEREF_SIZE;
    if (wdeRefCount == 0) {
      internalBytes = getBytesFromWdeRef(wdeRef);
      return;
    } else {
      // Use binary search to test if the WdeRef to add is already in the Set
      byte[] toAddBytes = getBytesFromWdeRef(wdeRef);
      int low = 0, high = wdeRefCount - 1;
      while (low <= high) {
        int mid = (low + high) / 2;
        int cmp = Bytes.compareTo(internalBytes, mid * WDEREF_SIZE, WDEREF_SIZE, toAddBytes, 0,
            WDEREF_SIZE);
        if (cmp == 0) {
          return;// already in the Set
        } else if (cmp < 0) {
          low = mid + 1;
        } else {
          high = mid - 1;
        }
      }
      // WdeRef is not in the Set, insert it!
      byte[] newArr = new byte[internalBytes.length + WDEREF_SIZE];
      // copy lower part
      if (low != 0) {
        System.arraycopy(internalBytes, 0, newArr, 0, low * WDEREF_SIZE);
      }
      // copy the wdeRef to add
      System.arraycopy(toAddBytes, 0, newArr, low * WDEREF_SIZE, WDEREF_SIZE);
      // copy higher part
      if (wdeRefCount - low != 0) {
        System.arraycopy(internalBytes, low * WDEREF_SIZE, newArr, low * WDEREF_SIZE + WDEREF_SIZE,
            (wdeRefCount - low) * WDEREF_SIZE);
      }
      internalBytes = newArr;
      return;
    }
  }
}
