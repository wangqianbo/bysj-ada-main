package ict.ada.gdb.model.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ReflectionTest {
private String a ;
private String b;
public Integer c;
public String getA() {
  return a;
}
public void setA(String a) {
  this.a = a;
}
public String getB() {
  return b;
}
public void setB(String b) {
  this.b = b;
}

/**
 * @param obj
 *            操作的对象
 * @param att
 *            操作的属性
 * */
public static void getter(Object obj, String att) {
    try {
        Method method = obj.getClass().getMethod("get" + att);
        System.out.println(method.invoke(obj));
    } catch (Exception e) {
        e.printStackTrace();
    }
}

/**
 * @param obj
 *            操作的对象
 * @param att
 *            操作的属性
 * @param value
 *            设置的值
 * @param type
 *            参数的属性
 * */
public static void setter(Object obj, String att, Object value,
        Class<?> type) {
    try {
        Method method = obj.getClass().getMethod("set" + att, type);
        method.invoke(obj, value);
    } catch (Exception e) {
        e.printStackTrace();
    }
}
public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchFieldException, SecurityException{
  Class<?> demo = Class.forName("ict.ada.gdb.model.util.ReflectionTest");
  Object obj = demo.newInstance();
  setter(obj,"A","男",String.class);
  getter(obj,"A");
  Field[] fields = demo.getDeclaredFields();
  for(Field field:fields){
    Class<?> type = field.getType();
    if(type == Integer.class)
    System.out.println(type);
    field.getName();
  System.out.println(field.get(obj));}
}
}
