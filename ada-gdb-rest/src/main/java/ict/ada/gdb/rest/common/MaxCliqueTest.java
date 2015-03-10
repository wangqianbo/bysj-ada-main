package ict.ada.gdb.rest.common;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class MaxCliqueTest {
  private int[][] graph;
  private short[] clique;
  private int length;
  private short numofvertex;
  private List<Clique> cliques;
  public double maxmem;
  public double mem;
  public double cliquesCount;

  public MaxCliqueTest(int[][] graph, short numofvertex) {
    this.graph = graph;
    this.numofvertex = numofvertex;
    clique = new short[numofvertex];
    length = 0;
    cliques = new LinkedList<Clique>();
    maxmem = 0;
    mem = 0;
    cliquesCount = 0;
  }

  public void genCliques() {
    // BackTrack1();
    int[] u = new int[numofvertex - 1];
    for (int i = 0; i < numofvertex - 1; i++)
      u[i] = i + 1;
    // cliqueM(u, numofvertex - 1);
    ArrayList<Integer>[] deg = new ArrayList[numofvertex - 1];
    for (int i = 1; i < numofvertex; i++) {
      int degreeofi = 0;
      for (int j = 1; j < numofvertex; j++)
        if (graph[i][j] == 1) degreeofi++;
      if (deg[numofvertex - degreeofi - 2] == null) deg[numofvertex - degreeofi - 2] = new ArrayList<Integer>();
      deg[numofvertex - degreeofi - 2].add(i);
    }
    ArrayList<Integer> X = new ArrayList<Integer>();
    int tomove = 0;
    for (ArrayList<Integer> indexlist : deg) {
      if (indexlist == null) continue;
      for (int index : indexlist) {
        int[] P = new int[numofvertex - 1];
        int p = 0;
        int count = 0;
        for (int v : u) {
          count++;
          if (v == 0) break;
          if (v == index) tomove = count;
          if (graph[index][v] == 1) P[p++] = v;
        }
        ArrayList<Integer> X1 = new ArrayList<Integer>();
        for (int x : X) {
          if (graph[index][x] == 1) X1.add(x);
        }
        this.clique[length++] = (short) index;
        BronKerbosch2(P, p, X1);
        for (int i = tomove; i < numofvertex - 1; i++) {
          if (u[i] == 0) {
            u[i - 1] = 0;
            break;
          }
          u[i - 1] = u[i];
        }
        u[numofvertex - 2] = 0;
        X.add(index);
        this.clique[--length] = 0;
      }
    }
  }

  public void genCliques1() {
    // BackTrack1();
    ArrayList<Short> u = new ArrayList<Short>();
    // int[] u = new int[numofvertex-1];
    for (short i = 0; i < numofvertex - 1; i++)
      u.add((short) (i + 1));
    // cliqueM(u, numofvertex - 1);
    version2(u, (short) (numofvertex - 1), (short) (numofvertex - 1));

  }

  private void version2(ArrayList<Short> oldMD, short oldCandidateSize, short oldTestedSize) {
    // int[] actualCandidates = new int[oldCandidateSize];
    ArrayList<Short> actualCandidates = null;
    short nod;
    short fixp = 0;
    short actualCandidateSize;
    short actualTestedSize;
    short i;
    short j;
    short count;
    short pos = 0;
    short p;
    short s = 0;
    short sel;
    short index2Tested;

    index2Tested = oldCandidateSize;
    nod = 0;
    mem += ((double) (12 * 2 + oldTestedSize * 2));
    if (maxmem < mem) maxmem = mem;
    // Determine each counter value and look for minimum
    // Branch and bound step
    // Is there a node in ND (represented by MD and index2Tested)
    // which is connected to all nodes in the candidate list CD
    // we are finished and backtracking will not be enabled
    for (i = (short) (oldTestedSize - 1); (i > 0) && (index2Tested != 0); i--) {
      p = oldMD.get(i);
      count = 0;

      // Count disconnections
      for (j = 0; (j < oldCandidateSize) && (count < index2Tested); j++) {
        if (graph[p][oldMD.get(j)] == 0) {
          count++;

          // Save position of potential candidate
          pos = j;
        }
      }

      // Test new minimum
      if (count < index2Tested) {
        fixp = p;
        index2Tested = count;

        if (i < oldCandidateSize) {
          s = i;
          // preincr
          nod = 1;
        } else {
          s = pos;
        }
      }
    }

    // If fixed point initially chosen from candidates then
    // number of diconnections will be preincreased by one
    // Backtracking step for all nodes in the candidate list CD
    for (nod = (short) (index2Tested + nod); nod >= 1; nod--) {
      // Interchange
      p = oldMD.get(s);
      oldMD.set(s, oldMD.get(oldCandidateSize - 1));
      oldMD.set(oldCandidateSize - 1, p);// 下一步放入not的
      // oldMD[s] = oldMD[oldTestedSize];
      sel = p;

      // Fill new set "not"
      actualCandidateSize = 0;
      actualCandidates = new ArrayList<Short>();
      for (i = 0; i < oldCandidateSize - 1; i++) {
        if (graph[sel][oldMD.get(i)] != 0) {
          actualCandidates.add(oldMD.get(i));
          actualCandidateSize++;
        }
      }

      // Fill new set "candidates"
      actualTestedSize = actualCandidateSize;

      for (i = oldCandidateSize; i < oldTestedSize; i++) {
        if (graph[sel][oldMD.get(i)] != 0) {
          actualCandidates.add(oldMD.get(i));
          actualTestedSize++;
        }
      }

      // Add to "actual relevant nodes"
      this.clique[this.length++] = sel;
      // actualMD.vertex[actualMD.size++] = sel;
      // so CD+1 and ND+1 are empty
      if (actualTestedSize == 0) {
        if (this.length < 2) return;
        this.cliques.add(new Clique(this.clique, this.length));
      } else {
        if (actualCandidateSize > 0) {
          version2(actualCandidates, actualCandidateSize, actualTestedSize);
        }
      }

      // if (fini) {
      // break;
      // }

      // move node from MD to ND
      // Remove from compsub
      this.clique[--length] = 0;

      // Add to "nod"
      oldCandidateSize--;

      if (nod > 1) {
        // Select a candidate disconnected to the fixed point
        for (s = (short) (oldCandidateSize - 1); graph[fixp][oldMD.get(s)] != 0; s--) {
        }
      }

      // end selection
    }

    // Backtrackcycle
    actualCandidates = null;
  }

  private void cliqueM(int[] u, int size) {
    if (size == 0) {
      addClique(this.clique);
      return;
    }

    for (int i = 0; i < size; i++) {
      clique[length++] = (short) u[i];
      int[] u1 = new int[size - i - 1];
      int size1 = 0;
      for (int j = i + 1; j < size; j++)
        if (graph[u[i]][u[j]] == 1) u1[size1++] = u[j];
      cliqueM(u1, size1);
      clique[--length] = 0;
    }
  }

  private void BronKerbosch2(int[] P, int p, ArrayList<Integer> X) {
    if (p == 0 && X.isEmpty()) {
      if (this.length < 2) return;
      Clique cli = new Clique(this.clique, this.length);
      this.cliques.add(cli);
    }
    if (p != 0) // choose a pivot. 有没有好的策略 现在取P[0]
    {
      for (int i = p - 1; i >= 0; i--) {
        if (graph[P[i]][P[0]] == 0) {
          this.clique[length++] = (short) P[i];
          int[] P1 = new int[p];
          int p1 = 0;
          for (int j = p - 1; j >= 0; j--) {
            if (graph[P[i]][P[j]] == 1) P1[p1++] = P[j];
          }
          ArrayList<Integer> X1 = new ArrayList<Integer>();
          for (int x : X) {
            if (graph[P[i]][x] == 1) X1.add(x);
          }
          BronKerbosch2(P1, p1, X1);
          X.add(P[i]);
          P[i] = P[--p];
          this.clique[--length] = 0;
        }

      }

    }
  }

  private void BackTrack(int i) {
    // System.out.println(i);

    if (i == numofvertex) {
      addClique(this.clique);
      return;
    }
    boolean OK = true;
    for (int j = 0; j < this.length; j++)
      if (graph[i][this.clique[j]] == 0) {
        OK = false;
        break;
      }
    if (OK) {
      clique[length++] = (short) i;
      BackTrack(i + 1);
      clique[--length] = 0;
    }
    BackTrack(i + 1);
  }

  private void BackTrack1() {
    clique[length++] = 0;
    int[] stack = new int[this.numofvertex + 1];
    boolean[] flag = new boolean[this.numofvertex + 1];
    int top = 0;
    stack[top] = 1;
    flag[top] = true;
    while (top != -1) {
      if (flag[top]) {
        flag[top] = false;
        if (stack[top] == numofvertex) {
          addClique(this.clique);
          clique[--length] = 0;
          top--;
          continue;
        }
        boolean OK = true;
        for (int j = 0; j < this.length; j++)
          if (graph[stack[top]][this.clique[j]] == 0) {
            OK = false;
            break;
          }
        if (OK) {
          clique[length++] = (short) stack[top];
          top++;
          flag[top] = true;
          stack[top] = stack[top - 1] + 1;
        }
      } else {
        flag[top] = true;
        stack[top] = stack[top] + 1;
      }

    }
  }

  private void addClique(short[] clique2) {
    if (this.length < 2) return;
    Clique cli = new Clique(clique2, length);
    if (!contains(cli)) this.cliques.add(cli);

  }

  private boolean contains(Clique cli) {
    boolean flag = false;
    for (Clique cli1 : this.cliques) {
      flag = cli1.contains(cli);
      if (flag) break;
    }
    return flag;
  }

  /**
   * @return the graph
   */
  public int[][] getGraph() {
    return graph;
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

  /**
   * @return the numofvertex
   */
  public int getNumofvertex() {
    return numofvertex;
  }

  /**
   * @return the cliques
   */
  public List<Clique> getCliques() {
    return cliques;
  }

  public static void main(String[] args) {
    // int[][] graph = { { 1, 1, 1, 1, 1, 1, 1, 1, 1 }, { 1, 1, 1, 1, 1, 0, 0, 1, 0 }, { 1, 1, 1, 1,
    // 1, 1, 0, 0, 0 }, { 1, 1, 1, 1, 1, 0, 0, 0, 0 },{ 1, 1, 1, 1, 1, 0, 0, 0, 0 }, { 1, 0, 1, 0,
    // 0, 1, 1, 0, 0 }, { 1, 0, 0, 0, 0, 1, 1, 1, 0 }, { 1, 1, 0, 0, 0, 0, 1,1, 0 }, { 1, 0, 0, 0,
    // 0, 0, 0, 0, 1 } };
    int[][] graph = { { 1, 1, 1, 1, 1, 1, 1 }, { 1, 1, 1, 0, 0, 1, 0 }, { 1, 1, 1, 1, 0, 1, 0 },
        { 1, 0, 1, 1, 1, 0, 0 }, { 1, 0, 0, 1, 1, 1, 1 }, { 1, 1, 1, 0, 1, 1, 0 },
        { 1, 0, 0, 0, 1, 0, 1 } };
    MaxCliqueTest m = new MaxCliqueTest(graph, (short) 7);
    m.genCliques1();
    for (int i = 0; i < 7; i++) {
      for (int j = 0; j < 7; j++)
        System.out.print(graph[i][j]);
      System.out.println();
    }
    for (Clique cli : m.getCliques()) {
      for (int i = 0; i < cli.getLength(); i++)
        System.out.print(cli.getClique()[i]);
      System.out.println();
    }
  }
}