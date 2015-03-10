/**
 * 
 */
package ict.ada.common.util;

/**
 * @author forhappy
 * 
 */
public class GeoAddressTest {

  public static void main(String[] args) {
    String[] latlon = GeoAddress.getCoordinate("shanghai");
    System.out.println("Latitude: " + latlon[0]);
    System.out.println("Longitude: " + latlon[1]);
  }
}
