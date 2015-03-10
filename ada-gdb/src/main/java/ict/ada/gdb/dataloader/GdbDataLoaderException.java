package ict.ada.gdb.dataloader;

public class GdbDataLoaderException extends Exception {
  private static final long serialVersionUID = 8407719372579843224L;

  public GdbDataLoaderException(String msg) {
    super(msg);
  }

  public GdbDataLoaderException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
