package ict.ada.gdb.util;

import ict.ada.common.util.Hex;

/**
 * 
 * Wrap an AsyncFileWriter to log Node accesses
 * 
 */
public class NodeAccessTracer {

  private AsyncFileWriter writer;

  public NodeAccessTracer() {
    writer = new AsyncFileWriter("/tmp/", "gdb-nodetrace-test");
  }

  /**
   * Trace line format: |-- timestamp in seconds --| -- Node id hex string --|
   */
  public void trace(byte[] nodeid) {
    if (nodeid == null) return;
    writer.append(System.currentTimeMillis() / 1000 + "\t" + Hex.encodeHex(nodeid));
  }

  public void close() {
    writer.close();
  }

}
