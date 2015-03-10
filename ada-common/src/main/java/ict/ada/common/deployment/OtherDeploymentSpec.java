package ict.ada.common.deployment;

public class OtherDeploymentSpec extends DeploymentSpec {
  private final int wdeRefIdLen = 16;

  @Override
  public int getWdeRefIdLen() {
    return wdeRefIdLen;
  }

}
