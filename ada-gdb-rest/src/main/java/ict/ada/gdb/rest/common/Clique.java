package ict.ada.gdb.rest.common;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;

public class Clique {// 可以先采用这种方法，或者用byte数组，或者boolean数组。
  private short[] clique;
  private int length;

  public Clique(short[] clique, int length) {
    this.clique = new short[length];
    System.arraycopy(clique, 0, this.clique, 0, length);
    this.length = length;
  }

  public boolean contains(Clique cli) {
    if (cli.length >= this.length) return false;
    else {
      boolean flag = true;
      int indexA = 0;
      for (int i = 0; i < cli.length; i++) {
        if (!flag) return flag;
        if (indexA == length) return false;
        if (cli.clique[i] == clique[indexA]) indexA++;
        else if (cli.clique[i] > clique[indexA]) for (; indexA < this.length; indexA++) {
          if (cli.clique[i] == clique[indexA]) {
            indexA++;
            break;
          } else if (cli.clique[i] < clique[indexA]) {
            flag = false;
            break;
          }

        }
        if (indexA == this.length) if (i == cli.length - 1 && cli.clique[i] == clique[indexA - 1]) return true;
        else return false;
      }
      return flag;
    }
  }

  /**
   * @return the clique
   */
  public short[] getClique() {
    return clique;
  }

  /**
   * @return the length
   */
  public int getLength() {
    return length;
  }

  public boolean bigThan(Clique clique) {
    return this.length > clique.length;
  }

  public static void main(String[] args) {
    long start = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      int[] a = new int[10000000];
      for (int j = 0; j < 10000000; j++)
        a[j] = j;
    }
    System.out.println(System.currentTimeMillis() - start);
    start = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      List<Integer> b = new ArrayList<Integer>();
      for (int j = 0; j < 10000000; j++)
        b.add(j);
    }
    System.out.println(System.currentTimeMillis() - start);
  }

}
