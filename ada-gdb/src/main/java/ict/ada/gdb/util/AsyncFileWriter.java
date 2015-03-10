package ict.ada.gdb.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A separate thread that writes data into a file asynchronously.
 * Used in Node access tracing and graph statistics collecting
 */
public class AsyncFileWriter {

  private final BlockingQueue<String> queue;
  private final File file;
  private Thread writerThread;
  private volatile boolean closed = false;
  private volatile boolean enableHook = true;

  /**
   * File is created with name= fileNamePrefix.|--current timestamp in sec--|.dat
   * 
   * @param dir
   * @param fileNamePrefix
   */
  public AsyncFileWriter(final String dir, String fileNamePrefix) {
    queue = new ArrayBlockingQueue<String>(500, false);
    file = new File(dir, fileNamePrefix + "." + System.currentTimeMillis() / 1000 + ".dat");
    writerThread = new Thread(new Runnable() {
      @Override
      public void run() {
        if (file.exists()) {
          throw new RuntimeException(file.getAbsolutePath() + ". Already exist?");
        }
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
          fw = new FileWriter(file);
          writer = new BufferedWriter(fw);
          final BufferedWriter writerCopy = writer;
          // Add shutdown hook to close file if program is killed by ctrl-c
          Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
              try {
                if (enableHook) {
                  writerCopy.close();
                  System.out.println("Shutdown Hook closed " + file.getAbsolutePath());
                } else {
                  System.out.println("Shutdow Hook ignored " + file.getAbsolutePath());
                }
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          });
          while (!Thread.interrupted()) {
            String content = queue.take();
            writer.append(content);
            writer.newLine();
          }
          consumeRemainingData(queue, writer);
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) { // exception from take()
          consumeRemainingData(queue, writer);
        } finally {
          try {
            System.out.println("Closing " + file.getAbsolutePath() + " ...");
            enableHook = false;// close writer below, so don't bother shutdown hook
            if (writer != null) writer.close();
            if (fw != null) fw.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }, "AsyncFileWriter " + file.getAbsolutePath());
    writerThread.start();
  }

  private static void consumeRemainingData(BlockingQueue<String> q, BufferedWriter writer) {
    String content;
    while (null != (content = q.poll())) {// consume remaining data
      try {
        writer.append(content);
        writer.newLine();
      } catch (IOException e1) {
        e1.printStackTrace();
        break;
      }
    }
  }

  /**
   * Thread-safe
   * 
   * @param content
   *          content for a line, a "\n" will be appended
   */
  public void append(String content) {
    if (content == null || content.length() == 0) return;
    if (closed) return;
    try {
      queue.put(content);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Thread-safe
   */
  public void close() {
    closed = true;
    writerThread.interrupt();
  }

  public static void main(String[] args) throws InterruptedException {
    AsyncFileWriter w = new AsyncFileWriter("D:\\", "nodetrace");
    w.append("xxx");
    w.append("dfasdf");
    // Thread.sleep(1000);
    w.close();
  }

}
