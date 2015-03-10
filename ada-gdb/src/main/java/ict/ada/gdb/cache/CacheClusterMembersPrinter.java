package ict.ada.gdb.cache;

import java.util.Arrays;

public class CacheClusterMembersPrinter {

  private void show() {
    CacheFacade gdbcache = CacheFacade.get();
    String[] cacheNodes = gdbcache.getCacheNodesAddrs();
    System.out.println("Cache Cluster Members Array: " + Arrays.toString(cacheNodes));

    System.out.println();
    System.out.println("=== Cache Servers' Status ===");
    gdbcache.requestAndPrintAllStatus();
  }

  // TODO ping for status

  public static void main(String[] args) {
    new CacheClusterMembersPrinter().show();
    System.exit(0);
  }

}
