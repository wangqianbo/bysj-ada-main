package ict.ada.gdb.rest.common;

public class NodeMinHeap {
  private NodeDegree[] heap;
  private int size;// 容量
  private int length;// 已使用的大小

  public NodeMinHeap(int size) {
    heap = new NodeDegree[size + 1];
    this.size = size;
    this.length = size;
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
    if (left != -1 && heap[i].bigThan(heap[left])) {
      min = left;
    } else {
      min = i;
    }
    if (right != -1 && heap[min].bigThan(heap[right])) {
      min = right;
    }
    if (min == i) {
      return;
    } else {
      NodeDegree temp = heap[i];
      heap[i] = heap[min];
      heap[min] = temp;
      min_heapify(min);
    }
  }

  public void addNodeDegree(NodeDegree path) {
    if (length == 0) {
      if (path.bigThan(heap[1])) {
        heap[1] = path;
        min_heapify(1);
      }
    } else {
      heap[length] = path;
      // System.out.println(length);
      min_heapify(length);
      // System.out.println(length1);
      length = length - 1;
    }
  }

  /**
   * @return the heap
   */
  public NodeDegree[] getHeap() {
    return heap;
  }

  /**
   * @param heap
   *          the heap to set
   */
  public void setHeap(NodeDegree[] heap) {
    this.heap = heap;
  }

  /**
   * @return the size
   */
  public int getSize() {
    return size;
  }

  /**
   * @param size
   *          the size to set
   */
  public void setSize(int size) {
    this.size = size;
  }

}
