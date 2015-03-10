package ict.ada.gdb.rest.test;

import ict.ada.gdb.model.util.PojoMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.javatuples.Pair;

public class TestMain {
  private static final int TASKS_COUNT = 10;
  private static String basePath = "/home/test/in/";
  private static String outPath = "/home/test/out/";
  private static BlockingQueue<StatisticsInfoBean> logs = new LinkedBlockingQueue<StatisticsInfoBean>();
  private static BlockingQueue<Pair<Integer,String>> query1 = new LinkedBlockingQueue<Pair<Integer,String>>();
  private static BlockingQueue<Pair<Integer,String>> query2 = new LinkedBlockingQueue<Pair<Integer,String>>();
  private static BlockingQueue<Pair<Integer,String>> query3 = new LinkedBlockingQueue<Pair<Integer,String>>();
  private static BlockingQueue<Pair<Integer,String>> query5 = new LinkedBlockingQueue<Pair<Integer,String>>();

  private static ExecutorService executorService = Executors.newFixedThreadPool(TASKS_COUNT*4);
  public static void main(String[] args) throws IOException {
    
    if(args == null || args.length != 1){
      //TODO print Exception and exit;
      System.exit(1);
      }
    System.out.println(args[0]);
     String path = basePath + args[0];
     String outpath = outPath + args[0]; 
     File dir = new File(path);
     if(dir.exists() && dir.isDirectory()){
       for(File file : dir.listFiles()){
         String fileName = file.getName();
         if(fileName.equals("1") || fileName.equals("2") || fileName.equals("3") || fileName.equals("5")){
          int type = Integer.parseInt(fileName);
          try {
            Scanner cin = new Scanner(file, "UTF-8");
            while(cin.hasNext()){
              if(type == 1)
                query1.add(new Pair<Integer,String>(type,cin.nextLine()));
              else if(type == 2)
                query2.add(new Pair<Integer,String>(type,cin.nextLine()));
              else if(type == 3)
                query3.add(new Pair<Integer,String>(type,cin.nextLine()));
              else if(type == 5)
                query5.add(new Pair<Integer,String>(type,cin.nextLine()));
            }
          } catch (FileNotFoundException e) {
           
            e.printStackTrace();
          }
         }else {
           continue;
         }
       }
     }else{
       System.err.println("Can not find  input file!");
     }
     FileWriter writer = new FileWriter(new File(outpath));
     
     writer.write("start:\t"+System.currentTimeMillis()+"\n");
    //TODO print start time;
    List<Future> states = new ArrayList<Future>(TASKS_COUNT*4);
    for(int i = 0; i < TASKS_COUNT; i++){
      Thread task1 = new Thread(new TestRunner(query1,logs),"query1_"+i);
      states.add(executorService.submit(task1));
      Thread task2 = new Thread(new TestRunner(query2,logs),"query2_"+i);
      states.add(executorService.submit(task2));
      Thread task3 = new Thread(new TestRunner(query3,logs),"query3_"+i);
      states.add(executorService.submit(task3));
      Thread task5 = new Thread(new TestRunner(query5,logs),"query5_"+i);
      states.add(executorService.submit(task5));
    }
   for(Future state : states){
     try {
      state.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
   }
   long end = System.currentTimeMillis();
   for(StatisticsInfoBean bean : logs){
     writer.write(PojoMapper.toJson(bean, false)+"\n");
   }
   writer.write("end:\t"+end);
   writer.close();
   executorService.shutdown();
   System.exit(0);
  }

}
