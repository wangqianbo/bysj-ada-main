package ict.ada.common.deployment;

public class AdaDeploymentSpec extends DeploymentSpec {
  private final int wdeRefIdLen = 8;

  @Override
  public int getWdeRefIdLen() {
    return wdeRefIdLen;
  }

}
