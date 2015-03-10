package ict.ada.common.deployment;


import java.io.IOException;
import java.util.Properties;

public class AdaCommonConfig {

  public static final DeploymentSpec DEPLOYMENTSPEC;

  /** Property file name */
  public static final String PROP_FILE_NAME = "ada_common_config.properties";

  static {
    Properties config = new Properties();
    try {
      config.load(DeploymentSpec.class.getClassLoader().getResourceAsStream(PROP_FILE_NAME));
      DEPLOYMENTSPEC = DeploymentSpec.createDeploymentSpec(getAndCheckString(config,"delployment.spec.class"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static boolean getAndCheckBoolean(Properties config, String key) throws IOException {
    return Boolean.parseBoolean(getAndCheckString(config, key));
  }

  private static String getAndCheckString(Properties config, String key) throws IOException {
    String value = config.getProperty(key);
    if (value == null || value.trim().length() == 0)
      throw new IOException("Invalid configuration. key=" + key + " value=" + value);
    return value;
  }

  public static void main(String[] args) {}

}
