package ict.ada.gdb.maptools;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;

public class MapNodeConfiguration {

    public MapNodeConfiguration() {
        super();
    }

    public MapNodeConfiguration(String config) {
        super();
        this.config = config;
        Properties props = new Properties();
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(
                    config));
            props.load(in);
            Enumeration<?> en = props.propertyNames();
            while (en.hasMoreElements()) {
                String key = (String) en.nextElement();
                String property = props.getProperty(key);
                if (key.equalsIgnoreCase("ada.update.timestamp")) {
                  lastUpdateTimestamp = property;
                } else if (key.equalsIgnoreCase("ada.hbase.server.ip")) {
                  adaHbaseServerIp = property;
                } else if (key.equalsIgnoreCase("ada.hbase.server.port")) {
                  adaHbaseServerPort = property;
                }
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @return the config
     */
    public String getConfig() {
        return config;
    }

    /**
     * @param config
     *            the config to set
     */
    public void setConfig(String config) {
        this.config = config;
        Properties props = new Properties();
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(
                    config));
            props.load(in);
            Enumeration<?> en = props.propertyNames();
            while (en.hasMoreElements()) {
                String key = (String) en.nextElement();
                String property = props.getProperty(key);
                if (key.equalsIgnoreCase("ada.update.timestamp")) {
                  lastUpdateTimestamp = property;
                } else if (key.equalsIgnoreCase("ada.hbase.server.ip")) {
                  adaHbaseServerIp = property;
                } else if (key.equalsIgnoreCase("ada.hbase.server.port")) {
                  adaHbaseServerPort = property;
                }
            }
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void setLastUpdateTimestamp() {
      Properties props = new Properties();
      try {
          InputStream in = new BufferedInputStream(new FileInputStream(
                  config));
          props.load(in);
          int current = (int) (System.currentTimeMillis() / 1000);
          System.out.println(current);
          props.setProperty("ada.update.timestamp", String.valueOf(current));
          in.close();
          
          OutputStream out = new BufferedOutputStream(new FileOutputStream(
              config));
          props.store(out, null);
          out.flush();
          out.close();
      } catch (Exception e) {
          e.printStackTrace();
      }
    }
    
    /**
     * @return the lastUpdateTimestamp
     */
    public String getLastUpdateTimestamp() {
      return lastUpdateTimestamp;
    }

    /**
     * @return the adaHbaseServer
     */
    public String getAdaHbaseServerIp() {
      return adaHbaseServerIp;
    }

    /**
     * @return the adaHbaseServerPort
     */
    public String getAdaHbaseServerPort() {
      return adaHbaseServerPort;
    }

    /**
     * private variables.
     */
    private String lastUpdateTimestamp = null;
    private String adaHbaseServerIp = null;
    private String adaHbaseServerPort = null;
    
    /**
     * configuration filepath.
     */
    private String config = null;

}
