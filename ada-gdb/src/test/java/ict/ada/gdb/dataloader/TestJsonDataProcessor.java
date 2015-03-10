package ict.ada.gdb.dataloader;

import static org.junit.Assert.*;

import ict.ada.common.model.Edge;
import ict.ada.common.model.Node;
import ict.ada.gdb.common.GdbException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Scanner;

import org.junit.Ignore;
import org.junit.Test;

public class TestJsonDataProcessor {

  @Test
  //@Ignore
  public void test() {
    GdbJsonDataProcessor processor = new GdbJsonDataProcessor(
        new GdbJsonDataProcessor.DataOpHandler() {
          @Override
          public void handle(DataOperation dataOp) throws GdbException {
            switch (dataOp.getType()) {
            case ADD_NODE:
              Node node = (Node) dataOp.getData();
              System.out.println(node.getName());
              System.out.println(Arrays.toString(node.getSnames().toArray(new String[0])));
              break;
            case ADD_EDGE:
              Edge edge = (Edge) dataOp.getData();
              System.out.println("[Head]" + edge.getHead().getName());
              System.out.println("[Tail]" + edge.getTail().getName());

              // System.out.println(edge.getRelations().get(0).getId());
              // assertTrue(edge.getHead().getName().contains("\u0001"));
              // System.out.println(edge.getHead().getName().contains("\u0001"));
              System.out.println();
              break;
            default:
              throw new IllegalStateException("Unknown DataOpType");
            }
          }

          @Override
          public void onFinish() throws GdbException {
            // TODO Auto-generated method stub
          }
        });
    String[] files = { "sample-json-file.dat",
    // "news/part-00000"
    // "activity", "appear",
    // "author-organization-zh_cn",
    // "coauthor", "coauthor-zh_cn",
    // "jointly-attend"
    };
    for (String file : files) {
      process(processor, file);
    }

  }

  private void process(GdbJsonDataProcessor processor, String path) {
    InputStream is = this.getClass().getClassLoader().getResourceAsStream(path);
    assertNotNull(is);
    Scanner cin = new Scanner(is);
    try {
      while (cin.hasNext()) {
        String line = cin.nextLine();
        try {
          processor.process(line);
        } catch (GdbDataLoaderException e) {
          System.err.println(line);
          e.printStackTrace();
          return;
        }
      }
    } finally {
      cin.close();
    }
  }
}
