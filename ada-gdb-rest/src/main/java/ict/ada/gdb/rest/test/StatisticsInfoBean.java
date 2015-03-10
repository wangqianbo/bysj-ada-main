package ict.ada.gdb.rest.test;

import java.util.HashMap;
import java.util.Map;

public class StatisticsInfoBean {
private int 	type;
private Map<String,String> args;
private Map<String,String> statistics;
private long ts;
private long dur;

public void addArg(String key,String value){
  if(args == null)
    args = new HashMap<String,String>();
  args.put(key, value);
}

public void addStatistic(String key,String value){
  if(statistics == null)
    statistics = new HashMap<String,String>();
  statistics.put(key, value);
}

public int getType() {
  return type;
}

public void setType(int type) {
  this.type = type;
}

public Map<String, String> getArgs() {
  return args;
}
public void setArgs(Map<String, String> args) {
  this.args = args;
}
public Map<String, String> getStatistics() {
  return statistics;
}
public void setStatistics(Map<String, String> statistics) {
  this.statistics = statistics;
}
public long getTs() {
  return ts;
}
public void setTs(long ts) {
  this.ts = ts;
}
public long getDur() {
  return dur;
}
public void setDur(long dur) {
  this.dur = dur;
}

}
