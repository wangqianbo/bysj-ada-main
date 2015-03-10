import ict.ada.gdb.rest.services.InternalServiceResources;
import ict.ada.gdb.service.AdaGdbService;

public class LocationTest {
  static AdaGdbService adaGdbService = InternalServiceResources.getAdaGdbService();

  public static void main(String[] args) {
    String name = "DC01风波亭";
    // System.out.println(adaGdbService.getLocationByName(name.getBytes()));
  }
}
