package ict.ada.gdb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Provides a framework for a multi-thread parallel task.
 * 
 * @param <V>
 *          the return type of Callable
 */
public abstract class ParallelTask<V> {

  private ExecutorService exec;
  private List<Future<V>> futures = new ArrayList<Future<V>>();

  /**
   * The thread pool to use
   * 
   * @param exec
   */
  public ParallelTask(ExecutorService exec) {
    this.exec = exec;
  }

  /**
   * Submit a task in thread pool
   * 
   * @param call
   */
  public void submitTasks(Callable<V> call) {
    Future<V> future = exec.submit(call);
    futures.add(future);
  }

  /**
   * Wait and gather all submitted tasks' results.<br>
   * 
   * @throws Exception
   */
  public void gatherResults() throws Exception {
    for (Future<V> future : futures) {// gather results
      processResult(future.get());
    }
  }

  /**
   * Process the result from one finished thread.<br>
   * This method will be called sequentially for each submitted task.
   * 
   * @param result
   */
  public abstract void processResult(V result);

}
