package ict.ada.gdb.common;

import ict.ada.common.model.NodeType.Channel;


public class AdaModeConfigTest {

  public static void main(String[] args) throws InterruptedException {
    StringBuilder dbConf = new StringBuilder();
    StringBuilder indexConf = new StringBuilder();
    AdaModeConfig.setMode(AdaModeConfig.GDBMode.INSERT);

    for (Channel channel : Channel.values()) {
      if (!channel.equals(Channel.values()[0])) {
        dbConf.append(",");
        indexConf.append(",");
      }
      dbConf.append(channel.getIntForm() + ":" + AdaModeConfig.getDBVersion(channel));
      indexConf.append(channel.getIntForm() + ":" + AdaModeConfig.getIndexNumber(channel));
    }
    System.out.println(indexConf.toString());
    AdaModeConfig.loadConfig(dbConf.toString(), indexConf.toString());
  }
}
