package ict.ada.gdb.util;

/**
 * 
 * a utility class for printing java.class.path in stdout
 * 
 */
public class ClasspathPrinter {

  public static void main(String[] args) {
    System.out.println(System.getProperty("java.class.path"));
  }

}
