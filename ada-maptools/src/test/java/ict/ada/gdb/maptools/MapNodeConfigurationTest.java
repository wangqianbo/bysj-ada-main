/**
 * 
 */
package ict.ada.gdb.maptools;

/**
 * @author forhappy
 *
 */
public class MapNodeConfigurationTest {

  /**
   * @param args
   */
  public static void main(String[] args) {
    while (true) {
      MapNodeConfiguration mapNodeConf = new MapNodeConfiguration("conf/ada-mapnode.properties");
      System.out.println(mapNodeConf.getAdaHbaseServerIp());
      System.out.println(mapNodeConf.getAdaHbaseServerPort());
      System.out.println(mapNodeConf.getLastUpdateTimestamp());
      mapNodeConf.setLastUpdateTimestamp();
      break;
    }
  }

}
