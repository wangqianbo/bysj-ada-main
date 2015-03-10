package ict.ada.gdb.rest.common;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

public class MinHeap<T> {
  private T[] heap;
  private int size;// 容量
  private int length;// size -length为已使用的大小.
  private Comparator<T> comparator;

  @SuppressWarnings("unchecked")
  public MinHeap(int size, Comparator<T> comparator) {
    this.size = size;
    this.length = size;
    this.comparator = comparator;
    heap = (T[]) new Object[size + 1];
  }

  public MinHeap(T[] heap, int size, Comparator<T> comparator) {

    this.heap = heap;
    this.size = size;
    this.length = size;
    this.comparator = comparator;
  }

  public int parent(int i) {
    return i == 1 ? -1 : i / 2;
  }

  public int lchild(int i) {
    int lchild = i * 2;
    return lchild > size ? -1 : lchild;
  }

  public int rchild(int i) {
    int rchild = i * 2 + 1;
    return rchild > size ? -1 : rchild;
  }

  private void min_heapify(int i) {
    int left = lchild(i);
    int right = rchild(i);
    int min = i;
    if (left != -1 && comparator.compare(heap[i], heap[left]) > 0) {
      min = left;
    } else {
      min = i;
    }
    if (right != -1 && comparator.compare(heap[min], heap[right]) > 0) {
      min = right;
    }
    if (min == i) {
      return;
    } else {
      T temp = heap[i];
      heap[i] = heap[min];
      heap[min] = temp;
      min_heapify(min);
    }
  }

  @SuppressWarnings("unchecked")
  public void addElement(T element) {

    if (length == 0) {
      if (comparator.compare(element, heap[1]) > 0) {
        heap[1] = element;
        min_heapify(1);
      }
    } else {
      heap[length] = element;
      // System.out.println(length);
      min_heapify(length);
      // System.out.println(length1);
      length = length - 1;
    }
  }

  /**
   * @return the heap
   */
  public T[] getHeap() {
    return heap;
  }

  public List<T> getSortedHeap() {
    List<T> copy = new ArrayList<T>(size);
    for (T t : heap) {
      if (t != null) copy.add(t);
    }
    Collections.sort(copy, comparator);
    LinkedList<T> result = new LinkedList<T>();
    for (T t : copy)
      result.add(0, t);
    return result;
  }

  public static void main(String[] args) {

    Integer t = new Integer(0);
    MinHeap<Integer> a = new MinHeap<Integer>(10, new Comparator<Integer>() {
      public int compare(Integer s1, Integer s2) {
        return s1 - s2;
      }

    });

    for (int i = 0; i < 1000; i++) {
      a.addElement(i);
    }
    for (Integer ele : a.getSortedHeap()) {
      if (ele != null) System.out.println(ele);
    }
  }
}
