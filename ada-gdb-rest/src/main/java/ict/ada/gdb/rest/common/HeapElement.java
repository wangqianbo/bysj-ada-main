package ict.ada.gdb.rest.common;

public class HeapElement<T> implements Comparable {
  private T element;

  public HeapElement() {

  }

  /**
   * @return the element
   */
  public T getElement() {
    return element;
  }

  @Override
  public int compareTo(Object o) {
    // TODO Auto-generated method stub
    return 0;
  }

}
