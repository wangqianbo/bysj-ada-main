package ict.ada.gdb.rest.util;

import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.rest.services.InternalServiceResources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventUtil {
  private static final List<Integer> methodIntType = InternalServiceResources.getMethodIntType();
  private static final List<String> methodStringType =InternalServiceResources.getMethodStringType();
  private static final List<String>methodName = InternalServiceResources.getMethodName();
  
  public  static final List<Integer> channelIntType = InternalServiceResources.getChannelIntType();
  private static final List<String> channelStringType =InternalServiceResources.getChannelStringType();
  private static final List<String> channelName = InternalServiceResources.getChannelName();
//  private static final List<Channel> channel = new ArrayList<Channel>();
  private static Map<Integer, String> methodIntToStringType;
  private static Map<Integer, String> methodIntToName;
  private static Map<String, Integer> methodStringTypeToIntType;

  static {
    methodIntToStringType = new HashMap<Integer, String>();
    methodIntToName = new HashMap<Integer, String>();
    methodStringTypeToIntType = new HashMap<String, Integer>();

    int i = 0;
    for (int intType : methodIntType) {
      methodIntToName.put(intType, methodName.get(i));
      methodStringTypeToIntType.put(methodStringType.get(i), intType);
      methodIntToStringType.put(intType, methodStringType.get(i));
      i++;
    }
  }

  private static Map<Integer, String> channelIntToStringType;
  private static Map<Integer, String> channelIntToName;
  private static Map<String, Integer> channelStringTypeToIntType;
  private static Map<Integer, Channel> channelIntToChannel;
  static {
    channelIntToStringType = new HashMap<Integer, String>();
    channelIntToName = new HashMap<Integer, String>();
    channelStringTypeToIntType = new HashMap<String, Integer>();
    channelIntToChannel = new HashMap<Integer, Channel>();
    int i = 0;
    for (int intType : channelIntType) {
      channelIntToName.put(intType, channelName.get(i));
      channelStringTypeToIntType.put(channelStringType.get(i), intType);
      channelIntToChannel.put(intType, Channel.getChannel(intType));
      channelIntToStringType.put(intType, channelStringType.get(i));
      i++;
    }

  }

  public static String getMethodName(int intType) {
    return methodIntToName.get(intType);
  }

  public static String getMethodStringType(int intType) {
    return methodIntToStringType.get(intType);
  }

  public static int getMethodIntType(String stringType) {
    if (methodStringTypeToIntType.containsKey(stringType)) return methodStringTypeToIntType
        .get(stringType);
    else return -1;
  }

  public static String getChannelName(int intType) {
    return channelIntToName.get(intType);
  }

  public static String getChannelStringType(int intType) {
    return channelIntToStringType.get(intType);
  }

  public static int getChannelIntType(String stringType) {
    if (channelStringTypeToIntType.containsKey(stringType)) return channelStringTypeToIntType
        .get(stringType);
    else return -1;
  }
  
  public static Channel getChannel(int intType) {
    return channelIntToChannel.get(intType);
  }

  public static void main(String[] args) {
    getChannelName(11);
    System.out.println(getChannelName(3));
  }
}
