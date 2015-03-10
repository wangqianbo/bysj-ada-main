package ict.ada.gdb.maptools;

public class MapNodeDriver {

  /**
   * @param args
   */
  public static void main(String[] args) {
    while (true) {
      MapNodeUpdater mapNodeUpdater = new MapNodeUpdater();
      mapNodeUpdater.update();
      try {
        Thread.sleep(4000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

}
