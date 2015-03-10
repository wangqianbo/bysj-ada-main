package ict.ada.search;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.TreeMap;

import redis.clients.jedis.*;

import com.google.gson.*;

import cn.golaxy.yqpt2.dtsearch2.client.DTSearchClient;
import cn.golaxy.yqpt2.dtsearch2.client.DTSearchDoc;
import cn.golaxy.yqpt2.dtsearch2.client.DTSearchResult;

public class AdaSearchMQService {

  private String mqName = null;
  private String mqMembers = "IndexTypes";
  // lpop interval elements each time
  private int interval = 100;

  private String indexServerAddr = null;
  private DTSearchClient dtClient = null;

  private String mqServerAddr = null;
  private Jedis redisClient = null;

  public AdaSearchMQService(String indexServerAddr, String mqServerAddr, String mqName) {
    this(indexServerAddr, mqServerAddr, mqName, 100);
  }

  public AdaSearchMQService(String indexServerAddr, String mqServerAddr, String mqName, int interval) {

    System.out.println(mqName + " " + indexServerAddr + " " + mqServerAddr);
    this.mqName = mqName;
    this.interval = interval;

    this.indexServerAddr = indexServerAddr;
    this.dtClient = new DTSearchClient();
    this.dtClient.SetDTService(indexServerAddr);

    String[] ipPort = mqServerAddr.split(":");
    this.mqServerAddr = mqServerAddr;
    // large enough to prevent timeout
    redisClient = new Jedis(ipPort[0], Integer.parseInt(ipPort[1]), 123456789);
  }

  private String getMQListName(short indexType) {
    return this.mqName + "." + indexType;
  }

  private String getMQListLockName(short indexType) {
    return this.mqName + "." + indexType + ".lock";
  }

  private void clearAllLock() {

    // get all list name
    for (String indexTypeString : redisClient.smembers(this.mqMembers)) {
      String lockName = getMQListLockName(Short.parseShort(indexTypeString));

      if (redisClient.get(lockName) != null && !redisClient.get(lockName).equals("0")) {
        System.out.println("\033[33m" + lockName + " exists, and not 0.\033[39m");
      }
      redisClient.set(lockName, "0");
    }
  }

  private void lock(String lock, boolean verbose) throws InterruptedException {
    if (verbose) {
      System.out.print("\033[33m" + now() + " Trying to obtain " + lock + " ... \033[39m");
    }
    if (redisClient.exists(lock)) {
      while (!redisClient.getSet(lock, "1").equals("0")) {
        System.out.print("\033[33m.\033[39m");
        Thread.sleep(1000);
      }
    } else {
      redisClient.set(lock, "1");
    }
    if (verbose) {
      System.out.println();
    }
    if (verbose) {
      System.out.println("\033[33m" + now() + " Obtained " + lock + " ... \033[39m");
    }
  }

  private void unlock(String lock) {
    redisClient.set(lock, "0");
  }

  // just for test
  // not used in gdb-rest
  public void search(String sname, Channel ch, Attribute att) {
    String query =
        "[FIELD]( type, [FILTER]( [FIELD]( sname, \"" + sname + "\" ), ==, "
            + NodeType.getType(ch, att).getIntegerForm() + " ) )";
    DTSearchResult result = new DTSearchResult();
    List<String> fields = new LinkedList<String>();
    fields.add("name");
    fields.add("sname");
    fields.add("type");
    fields.add("addl");
    boolean reverse = false;
    dtClient.Search((short) ch.getIntForm(), query, fields, 0, 1000, reverse, result);
    System.out.println(result.ret_code);
    System.out.println(result.matchs);
    for (DTSearchDoc doc : result.docs) {
      for (String f : doc.fields) {
        System.out.print(" " + f);
      }
      System.out.println();
    }
  }

  public boolean deleteIndex(short indexType) {
    String listName = getMQListName(indexType);
    String lockName = getMQListLockName(indexType);
    try {
      lock(lockName, true);
    } catch (InterruptedException e) {
      e.printStackTrace();
      return false;
    }
    System.out.print("\033[33m Trying to delete ... \033[39m");

    Pipeline p = redisClient.pipelined();
    for (long i = 0, n = redisClient.llen(listName); i < n; ++i) {
      p.lpop(listName);
    }
    p.sync();
    boolean res = dtClient.ClearChannel(indexType);
    unlock(lockName);
    if (res) {
      System.out.println("\033[32m Done!\033[39m");
    } else {
      System.out.println("\033[31m Fail!\033[39m");
    }
    return res;
  }

  // pass it to MQ
  // indexType just consists of channel
  public boolean index(short indexType, String docs) {
    JsonObject obj = new JsonObject();
    obj.add("op", new JsonPrimitive("index"));
    obj.add("type", new JsonPrimitive(indexType));
    obj.add("docs", new JsonPrimitive(docs));
    // System.out.println("receive " + indexType + " " + docs);
    // what if fail, retry?
    String listName = getMQListName(indexType);
    redisClient.rpush(listName, obj.toString());
    redisClient.sadd(this.mqMembers, indexType + "");
    return true;
  }

  // LRANGE and push to DTSearch
  // if ok, LPOP
  public void run() throws InterruptedException {
    clearAllLock();

    long totalPost = 0, totalIndex = 0;
    JsonParser jp = new JsonParser();
    while (true) {
      int num = 0;
      for (String indexTypeString : redisClient.smembers(this.mqMembers)) {
        String listName = getMQListName(Short.parseShort(indexTypeString));
        String lockName = getMQListLockName(Short.parseShort(indexTypeString));

        lock(lockName, false);

        Map<Short, List<JsonElement>> docs = new TreeMap<Short, List<JsonElement>>();
        List<String> megs = redisClient.lrange(listName, 0, interval - 1);
        for (String meg : megs) {
          JsonObject json = jp.parse(meg).getAsJsonObject();
          short indexType = json.get("type").getAsShort();
          if (json.get("op").getAsString().equals("index")) {
            if (!docs.containsKey(indexType)) {
              docs.put(indexType, new ArrayList<JsonElement>());
            }
            for (JsonElement doc : jp.parse(json.get("docs").getAsString()).getAsJsonArray()) {
              docs.get(indexType).add(doc);
            }
          } else {
            System.out.println("\033[31m " + now() + " NO handler for message: " + json
                + "\033[39m");
          }
        }
        boolean allDone = true;
        for (Entry<Short, List<JsonElement>> entry : docs.entrySet()) {
          JsonArray jsonArr = new JsonArray();
          for (JsonElement doc : entry.getValue()) {
            jsonArr.add(doc);
          }
          if (!dtClient.Index(entry.getKey(), jsonArr.toString())) {
            System.out.println("\033[31m " + now() + " Index failed with indexType "
                + entry.getKey() + ". Redo later. \033[39m");
            allDone = false;
            break;
          } else {
            /*
             * System.out.println("\033[36m " + now() + " Index" + entry.getKey() + " " + jsonArr +
             * "  successfully\033[39m");
             */
            num += entry.getValue().size();
            totalIndex += entry.getValue().size();
            totalPost += 1;
            if (totalPost % 10 == 0) {
              System.out.println("\033[36m " + now() + " TotalIndex " + totalIndex + " TotalPost "
                  + totalPost + "\033[39m");
            }
          }
        }
        // if fail and redo, what about duplication?
        if (allDone) {
          Pipeline p = redisClient.pipelined();
          for (int i = 0; i < megs.size(); ++i) {
            p.lpop(listName);
          }
          p.sync();
        }
        unlock(lockName);
      }
      // The queue is empty, rest for a while
      if (num == 0) {
       Thread.sleep(10000);
      }
    }
  }

  private String now() {
    return new Date().toString();
  }

  public static void main(String[] args) throws InterruptedException {
    if (args.length != 1) {
      System.out.println("\033[31m args should be: property \033[39m");
      System.exit(-1);
    }
    Properties prop = new Properties();
    try {
      prop.load(new FileInputStream(args[0]));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    AdaSearchMQService mq =
        new AdaSearchMQService(prop.getProperty("ada.indexserver.address"),
            prop.getProperty("ada.mqserver.address"), prop.getProperty("ada.mqserver.mqname"),
            Integer.parseInt(prop.getProperty("ada.mqserver.interval")));

    mq.run();
  }
}
