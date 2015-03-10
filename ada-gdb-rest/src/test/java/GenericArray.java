public class GenericArray<T> {
  private Object[] array;

  public GenericArray(int sz) {
    array = new Object[sz];
  }

  public void put(int index, T item) {
    array[index] = item;
  }

  public T get(int index) {
    return (T) array[index];
  }

  public T[] rep() {
    return (T[]) array;
  }

  public static void main(String[] args) {
    GenericArray<Integer> array = new GenericArray<Integer>(10);
    for (int i = 0; i < 10; i++)
      array.put(i, i);
    for (int i = 0; i < 10; i++)
      System.out.print(array.get(i) + " ");
    System.out.println();
    Object[] ia = array.rep();
    for (Object o : ia)
      System.out.print((Integer) o + " ");
  }
}
