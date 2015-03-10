package ict.ada.gdb.rest.common;

import java.util.TreeMap;

public class MaxClique {
  private boolean[][] graph;
  private short[] clique;
  private int length;
  private int numofvertex;
  // private List<Clique> cliques;
  public int totalCliques = 0;
  private CliqueMinHeap cliques;
  private TreeMap<Integer, Integer> info; // 统计信息，{团点数：团数}

  public MaxClique(boolean[][] graph, int numofvertex, int maxCliques) {
    this.graph = graph;
    this.numofvertex = numofvertex;
    clique = new short[numofvertex];
    length = 0;
    cliques = new CliqueMinHeap(maxCliques);
    info = new TreeMap<Integer, Integer>();
  }

  public void genCliques() {
    // BackTrack1();
    short[] u = new short[numofvertex - 1];
    // int[] u = new int[numofvertex-1];
    for (short i = 0; i < numofvertex - 1; i++)
      u[i] = (short) (i + 1);
    // cliqueM(u, numofvertex - 1);
    version2(u, (short) (numofvertex - 1), (short) (numofvertex - 1));
  }

  private void version2(short[] oldMD, short oldCandidateSize, short oldTestedSize) {
    // int[] actualCandidates = new int[oldCandidateSize];
    short[] actualCandidates = null;
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
    // Determine each counter value and look for minimum
    // Branch and bound step
    // Is there a node in ND (represented by MD and index2Tested)
    // which is connected to all nodes in the candidate list CD
    // we are finished and backtracking will not be enabled
    for (i = (short) (oldTestedSize - 1); (i >= 0) && (index2Tested != 0); i--) {
      p = oldMD[i];
      count = 0;

      // Count disconnections
      for (j = 0; (j < oldCandidateSize) && (count < index2Tested); j++) {
        if (!graph[p][oldMD[j]]) {
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
    actualCandidates = new short[oldTestedSize];
    // If fixed point initially chosen from candidates then
    // number of diconnections will be preincreased by one
    // Backtracking step for all nodes in the candidate list CD
    for (nod = (short) (index2Tested + nod); nod >= 1; nod--) {
      // Interchange
      p = oldMD[s];
      oldMD[s] = oldMD[oldCandidateSize - 1];
      oldMD[oldCandidateSize - 1] = p;// 下一步放入not的
      // oldMD[s] = oldMD[oldTestedSize];
      sel = p;

      // Fill new set "not"
      actualCandidateSize = 0;
      for (i = 0; i < oldCandidateSize - 1; i++) {
        if (graph[sel][oldMD[i]]) {
          actualCandidates[actualCandidateSize++] = oldMD[i];

        }
      }

      // Fill new set "candidates"
      actualTestedSize = actualCandidateSize;

      for (i = oldCandidateSize; i < oldTestedSize; i++) {
        if (graph[sel][oldMD[i]]) {
          actualCandidates[actualTestedSize++] = oldMD[i];

        }
      }

      // Add to "actual relevant nodes"
      this.clique[this.length++] = sel;
      // actualMD.vertex[actualMD.size++] = sel;
      // so CD+1 and ND+1 are empty
      if (actualTestedSize == 0) {
        if (this.length >= 2) {
          totalCliques++;
          if (info.containsKey(this.length + 1)) info.put(this.length + 1,
              info.get(this.length + 1) + 1);
          else info.put(this.length + 1, 1);
          if (cliques.getMinCliqueSize() < this.length) {
            Clique clique = new Clique(this.clique, this.length);
            cliques.addClique(clique);
          }
        }
      } else {
        // if (actualCandidateSize >0&&actualCandidateSize+this.length>cliques.getMinCliqueSize()) {
        // //这步减枝条件使得统计信息不再准确。可以考虑去掉。
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
        for (s = (short) (oldCandidateSize - 1); graph[fixp][oldMD[s]]; s--) {
        }
      }

      // end selection
    }

    // Backtrackcycle
    actualCandidates = null;
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
  public CliqueMinHeap getCliques() {
    return cliques;
  }

  /**
   * @return the totalCliques
   */
  public int getTotalCliques() {
    return totalCliques;
  }

  /**
   * @param totalCliques
   *          the totalCliques to set
   */
  public void setTotalCliques(int totalCliques) {
    this.totalCliques = totalCliques;
  }

  /**
   * @return the info
   */
  public TreeMap<Integer, Integer> getInfo() {
    return info;
  }

  /**
   * @param info
   *          the info to set
   */
  public void setInfo(TreeMap<Integer, Integer> info) {
    this.info = info;
  }

  public static void main(String[] args) {
    // int[][] graph1 = { { 1, 1, 1, 1, 1, 1, 1, 1, 1 }, { 1, 1, 1, 1, 1, 0, 0, 1, 0 }, { 1, 1, 1,
    // 1, 1, 1, 0, 0, 0 }, { 1, 1, 1, 1, 0, 0, 0, 0, 0 },{ 1, 1, 1, 0, 1, 0, 0, 0, 0 }, { 1, 0, 1,
    // 0, 0, 1, 1, 0, 0 }, { 1, 0, 0, 0, 0, 1, 1, 1, 0 }, { 1, 1, 0, 0, 0, 0, 1,1, 0 }, { 1, 0, 0,
    // 0, 0, 0, 0, 0, 1 } };
    int[][] graph1 = { { 1, 1, 1, 1, 1, 1, 1 }, { 1, 1, 1, 0, 0, 1, 0 }, { 1, 1, 1, 1, 0, 1, 0 },
        { 1, 0, 1, 1, 1, 0, 0 }, { 1, 0, 0, 1, 1, 1, 1 }, { 1, 1, 1, 0, 1, 1, 0 },
        { 1, 0, 0, 0, 1, 0, 1 } };
    // int[][]
    // graph1={{1,1,1,1,1,0,0},{1,1,1,1,1,0,0,},{1,1,1,1,1,0,0,},{1,1,1,1,1,0,0,},{1,1,1,1,1,0,0,},{0,0,0,0,0,1,1,},{0,0,0,0,0,1,1}};
    // int[][]
    // graph1={{1,1,1,1,1,1,1,1},{1,1,1,0,0,0,0,0},{1,1,1,0,0,0,0,0},{1,0,0,1,0,0,0,0},{1,0,0,0,1,1,0,0},{1,0,0,0,1,1,1,1},{1,0,0,0,0,1,1,1},{1,0,0,0,0,1,1,1}};
    int v = 7;
    boolean[][] graph = new boolean[v][v];
    for (int i = 0; i < v; i++) {
      for (int j = 0; j < v; j++)
        graph[i][j] = (graph1[i][j] == 1) ? true : false;
    }
    MaxClique m = new MaxClique(graph, (short) v, 4);
    m.genCliques();

    for (Clique cli : m.getCliques().getHeap()) {
      if (cli == null) continue;
      for (int i = 0; i < cli.getLength(); i++)
        System.out.print(cli.getClique()[i]);
      System.out.println();
    }
  }
}
