package ict.ada.common.exception;

/**
 * Base class for any Exception in Ada system.
 */
public class AdaException extends Exception {
  private static final long serialVersionUID = -8564675586023019841L;

  public AdaException() {
    super();
  }

  public AdaException(String message) {
    super(message);
  }

  public AdaException(Throwable cause) {
    super(cause);
  }

  public AdaException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
