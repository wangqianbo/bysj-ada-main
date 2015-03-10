package ict.ada.gdb.model.util;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ReflectionUtil {
  private static Map<Class<?>, Map<String, Field>> classFieldsMap = new HashMap<Class<?>, Map<String, Field>>();

  public static String getField(Class<?> clazz, String fieldName, Object obj) {
    if (!clazz.isInstance(obj)) return null; // Throw a exception
    if (!classFieldsMap.containsKey(clazz)) registerClass(clazz);
    Field field = classFieldsMap.get(clazz).get(fieldName);
    if (field == null) return null;
    Class<?> type = field.getType();
    try {
      if (type == Date.class) {
        Object date = field.get(obj);
        if (date == null) return null;
        else return String.valueOf(((Date) date).getTime()/1000);
      } else return String.valueOf(field.get(obj));
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private static void registerClass(Class<?> clazz) {
    Field[] fields = clazz.getDeclaredFields();
    HashMap<String, Field> fieldMap = new HashMap<String, Field>();
    for (Field field : fields) {
      field.setAccessible(true);
      fieldMap.put(field.getName(), field);
    }
    classFieldsMap.put(clazz, fieldMap);
  }
}
