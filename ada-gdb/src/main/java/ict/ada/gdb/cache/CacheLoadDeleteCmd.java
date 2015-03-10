package ict.ada.gdb.cache;

/**
 * 
 * Command line tools to load/delete data in GdbCache
 * 
 */

public class CacheLoadDeleteCmd {

  private static final String DELETE = "DELETE";
  private static final String LOAD = "LOAD";

  public static void main(String[] args) {
    if (args.length < 3 || args.length > 4) {
      System.out
          .println("Usage: operation(DELETE|LOAD) filesystem(LOCAL|HDFS) node_id_file_path [CacheServer address(ip:port)]");
      System.exit(0);
    }

    String op = args[0];
    if (!op.equalsIgnoreCase(DELETE) && !op.equalsIgnoreCase(LOAD))
      throw new IllegalArgumentException("Invalid operation type( LOAD or DELETE): " + op);

    String fs = args[1];
    boolean localFs;
    if (fs.equalsIgnoreCase("HDFS")) localFs = false;
    else if (fs.equalsIgnoreCase("LOCAL")) localFs = true;
    else throw new IllegalArgumentException("Invalid filesystem type( HDFS or LOCAL): " + fs);

    String path = args[2];
    if (!path.startsWith("/")) throw new IllegalArgumentException("File path must start with /");

    String ipWithPort = null;
    if (args.length == 4) {
      ipWithPort = args[3];
      if (!ipWithPort.contains(":"))
        new IllegalArgumentException("CacheServer address must be like \"ip:port\"");
    }

    System.out.println("Arguments: "//
        + "\n operation=" + op//
        + "\n local_fs=" + localFs//
        + "\n node_id_file_path=" + path//
        + (args.length == 4 ? "\n server_addr=" + ipWithPort : "")//
        + "\n");

    CacheFacade gdbcache = CacheFacade.get();
    if (args.length == 4) {// submit to a single CacheServer
      boolean result = false;
      if (op.equalsIgnoreCase(DELETE)) {
        result = gdbcache.submitNodeIdFileToDeleteCache(ipWithPort, path, localFs);
      } else if (op.equalsIgnoreCase(LOAD)) {
        result = gdbcache.submitNodeIdFileToLoadCache(ipWithPort, path, localFs);
      } else {
        throw new RuntimeException("op=" + op);
      }
      System.out.println("CacheServer " + (result ? "Rejected the Task." : "Accepted the Task."));
    } else if (args.length == 3) {
      if (op.equalsIgnoreCase(DELETE)) {
        gdbcache.submitNodeIdFileToClusterToDeleteCache(path, localFs);
      } else if (op.equalsIgnoreCase(LOAD)) {
        gdbcache.submitNodeIdFileToClusterToLoadCache(path, localFs);
      }
    }
    System.exit(0);
  }
}
