package ict.ada.common.deployment;

import java.lang.reflect.Constructor;

public class DeploymentSpec {
  private static ClassLoader classLoader;
  private static final Class<?>[] EMPTY_ARRAY = new Class[] {};
  static {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = DeploymentSpec.class.getClassLoader();
    }
  }

  public int getWdeRefIdLen() {
    return 8;
  }

  @SuppressWarnings("unchecked")
  public static DeploymentSpec createDeploymentSpec(String className) {
    try {
      Class<?> clazz = Class.forName(className, true, classLoader);
      Constructor<DeploymentSpec> meth = (Constructor<DeploymentSpec>) clazz
          .getDeclaredConstructor(EMPTY_ARRAY);
      DeploymentSpec result = meth.newInstance();
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}