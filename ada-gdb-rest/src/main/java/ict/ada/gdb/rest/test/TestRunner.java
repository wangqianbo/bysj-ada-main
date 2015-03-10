package ict.ada.gdb.rest.test;

import java.util.concurrent.BlockingQueue;

import org.javatuples.Pair;

public class TestRunner implements Runnable {
  private BlockingQueue<Pair<Integer, String>> queryTask = null;
  private BlockingQueue<StatisticsInfoBean> logs  = null;
  
  public TestRunner(BlockingQueue<Pair<Integer, String>> queryTask, BlockingQueue<StatisticsInfoBean> logs) {
    this.queryTask = queryTask;
    this.logs = logs;
  }

  @Override
  public void run() {
    // TODO Auto-generated method stub
    while (!queryTask.isEmpty()) {
      Pair<Integer, String> query = queryTask.poll();
      int type = query.getValue0();
      String[] args = query.getValue1().split(",");
      StatisticsInfoBean result = null;
      try {
        switch (type) {
          case 1:
            result = TestActions.testGetAttr(args[0]);
            break;
          case 2:
            result = TestActions.testGetPathBetTwoNodes(args[0], args[1]);
            break;
          case 3:
            result =TestActions.testGetNodeRelById(args[0]);
            break;
          case 5:
            result = TestActions.testGetTwoLevelRelationGraph(args[0]);
            break;
          default:
            break;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      if(result != null)
        logs.offer(result);
    }
  }

}
