package ict.ada.gdb.rest.test;

import ict.ada.gdb.model.util.PojoMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

public class SumResult {

  private static List<StatisticsInfoBean> query1 = new ArrayList<StatisticsInfoBean>();
  private static List<StatisticsInfoBean> query2 = new ArrayList<StatisticsInfoBean>();
  private static List<StatisticsInfoBean> query3 = new ArrayList<StatisticsInfoBean>();
  private static List<StatisticsInfoBean> query5 = new ArrayList<StatisticsInfoBean>();
  private static Comparator<StatisticsInfoBean> comparator = new Comparator<StatisticsInfoBean>() {

    @Override
    public int compare(StatisticsInfoBean o1, StatisticsInfoBean o2) {
      return (int) (o1.getDur() - o2.getDur());
    }
  };


  private static void genResult(List<StatisticsInfoBean> query) {
    Collections.sort(query, comparator);
    List<StatisticsInfoBean> hasResult = new ArrayList<StatisticsInfoBean>();
    List<StatisticsInfoBean> hasNoResult = new ArrayList<StatisticsInfoBean>();
    for (StatisticsInfoBean bean : query) {
      switch (bean.getType()) {
        case 1:
          if (bean.getStatistics().get("attrCount") == null
              || bean.getStatistics().get("attrCount").equals("-1")
              || bean.getStatistics().get("attrCount").equals("0")) {
            hasNoResult.add(bean);
          } else {
            hasResult.add(bean);
          }
          break;
        case 2:
          if (bean.getStatistics().get("pathCount") == null
              || bean.getStatistics().get("pathCount").equals("-1")
              || bean.getStatistics().get("pathCount").equals("0")) {
            hasNoResult.add(bean);
          } else {
            hasResult.add(bean);
          }
          break;
        case 3:
          if (bean.getStatistics().get("relCount") == null
              || bean.getStatistics().get("relCount").equals("-1")
              || bean.getStatistics().get("relCount").equals("0")) {
            hasNoResult.add(bean);
          } else {
            hasResult.add(bean);
          }
          break;
        case 5:
          if (bean.getStatistics().get("edgeCount") == null
              || bean.getStatistics().get("edgeCount").equals("-1")
              || bean.getStatistics().get("edgeCount").equals("0")) {
            hasNoResult.add(bean);
          } else {
            hasResult.add(bean);
          }
          break;
        default:
          break;

      }
    }

    System.out.println("有结果查询数： " + hasResult.size() + "/" + query.size());
    System.out.println("=========有结果查询统计结果==========");
    if (hasResult.size() > 0) {
      double totalDur = 0;
      for (StatisticsInfoBean bean : hasResult) {
        totalDur += bean.getDur();
      }
      System.out.println("最大延迟： " + hasResult.get(hasResult.size() - 1).getDur());
      System.out.println("最小延迟：" + hasResult.get(0).getDur());
      System.out.println("延迟中位数： " + hasResult.get(hasResult.size() / 2).getDur());
      System.out.println("延迟90%分位数： " + hasResult.get((int) (hasResult.size() *0.9)).getDur());
      System.out.println("平均延迟： " + totalDur/hasResult.size());
    }
    System.out.println("=========没有结果查询统计结果==========");
    if(hasNoResult.size() > 0){
      double totalDur = 0;
      for (StatisticsInfoBean bean : hasNoResult) {
        totalDur += bean.getDur();
      }
      System.out.println("最大延迟： " + hasNoResult.get(hasNoResult.size() - 1).getDur());
      System.out.println("最小延迟：" + hasNoResult.get(0).getDur());
      System.out.println("延迟中位数： " + hasNoResult.get(hasNoResult.size() / 2).getDur());
      System.out.println("延迟90%分位数： " + hasNoResult.get((int) (hasNoResult.size() *0.9)).getDur());
      System.out.println("平均延迟： " + totalDur/hasNoResult.size());
    
    }
  }

  public static void main(String[] args) throws JsonMappingException, JsonParseException,
      IOException {
    if (args.length != 1) {
      System.err.println("Need input path!");
      System.exit(1);
    }
    File dir = new File(args[0]);
    double throughput = 0;
    double count = 0;
    if (dir.isDirectory()) {
      for (File file : dir.listFiles()) {
        long start = 0, end = 0;
        Scanner cin = new Scanner(file);
        while (cin.hasNextLine()) {
          String line = cin.nextLine();
          if (line.startsWith("start:")) {
            start = Long.parseLong(line.split("\t")[1]);
          } else if (line.startsWith("end:")) {
            end = Long.parseLong(line.split("\t")[1]);
          } else {
            StatisticsInfoBean bean =
                (StatisticsInfoBean) PojoMapper.fromJson(line, StatisticsInfoBean.class);
            count++;
            switch (bean.getType()) {
              case 1:
                query1.add(bean);
                break;
              case 2:
                query2.add(bean);
                break;
              case 3:
                query3.add(bean);
                break;
              case 5:
                query5.add(bean);
                break;
              default:
                break;
            }
          }
        }
        throughput += (count * 1000) / (end - start);
      }
    } else {

    }
    System.out.println("throughput = " + throughput);
    System.out.println("==========属性查询==========");
    genResult(query1);
    System.out.println("==========2跳路径查询==========");
    genResult(query2);
    System.out.println("==========邻接点查询==========");
    genResult(query3);
    System.out.println("==========2层邻接点==========");
    genResult(query5);
  }
}
