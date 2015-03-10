package ict.ada.dataprocess.community;

public class CommunityPair implements Comparable<CommunityPair> {
  private int indexA;
  private int indexB;
  private double distance;

  public CommunityPair(int indexA, int indexB, double distance) {
    setIndexA(indexA);
    setIndexB(indexB);
    setDistance(distance);
  }

  public int getIndexA() {
    return indexA;
  }

  public void setIndexA(int indexA) {
    this.indexA = indexA;
  }

  public int getIndexB() {
    return indexB;
  }

  public void setIndexB(int indexB) {
    this.indexB = indexB;
  }

  public double getDistance() {
    return distance;
  }

  public void setDistance(double distance) {
    this.distance = distance;
  }

  public int compareTo(CommunityPair o) {
    if (this.getDistance() < o.getDistance()) return -1;
    else if (this.getDistance() == o.getDistance()) return 0;
    else return 1;
  }

}
