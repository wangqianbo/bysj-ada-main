package ict.ada.common.util;

import ict.ada.common.deployment.DeploymentSpec;

public class Test {
public static void main(String[] args){
  DeploymentSpec test = DeploymentSpec.createDeploymentSpec("ict.ada.common.deployment.OtherDeploymentSpec");
  System.out.println(test.getWdeRefIdLen());
}
}
