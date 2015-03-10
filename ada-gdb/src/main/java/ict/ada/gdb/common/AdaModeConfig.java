package ict.ada.gdb.common;

import ict.ada.common.model.NodeType.Channel;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AdaModeConfig {
	private static final Logger LOG = LoggerFactory
			.getLogger(AdaModeConfig.class);

	private static GDBMode mode = GDBMode.INSERT;// 默认为入库状态
	private static final String INSERT_CONFIG_PATH = "/ada/gdb/load-config";
	private static final String QUERY_CONFIG_PATH = "/ada/gdb/query-config";
	private static String ZKPATH = "/ada/gdb/load-config";// 保持于mode一致.为入库状态.
	private static boolean watch = false;// 保持于mode一致.为入库状态不必观察变化.
	private static final int SESSION_TIMEOUT = 40*1000;//40s 
	private static final String CONNECTION_STRING = AdaConfig.CONNECTION_STRING;
	
	private static final ReadWriteLock lock=new ReentrantReadWriteLock();//由于每次都是整个更新,因此不存在性能问题,直接使用读写锁即可
	private static final Lock r=lock.readLock();
	private static final Lock w=lock.writeLock();
	
	private static HashMap<Channel, String> dbconfig = new HashMap<Channel, String>();
	private static HashMap<Channel, Integer> searchconfig = new HashMap<Channel, Integer>();
	private static ZKUtil zkUtil = new ZKUtil();

	
	/**
	 * @param dbConfigS "key1:value1,key2:value2,..." 其中key为通道号,value为版本(v1 v2)
	 * @param searchConfigs: "key1:value1,key2:value2,..." 其中key为通道号,value检索所用到的号(1 或 1001)
	 */
	public static void loadConfig(String dbConfigS,String searchConfigs){
		LOG.info("dbConfigS="+dbConfigS+"  searchConfigs="+searchConfigs);
		dbconfig.clear();
		searchconfig.clear();
		for(String kvs:dbConfigS.split(",")){
		  if (kvs == null || kvs.length() == 0) {
		    continue;
		  }
			String[] kv =kvs.split(":");
			dbconfig.put(Channel.getChannel(Integer.parseInt(kv[0])),
					kv[1]);}
		for(String kvs:searchConfigs.split(",")){
		  if (kvs == null || kvs.length() == 0) {
            continue;
          }
			String[] kv =kvs.split(":");
			searchconfig.put(Channel.getChannel(Integer.parseInt(kv[0])),
					Integer.parseInt(kv[1]));}
	}
	
	/**
	 * 加载zookeeper中的配置.
	 */
	public static void loadConfig() {
		w.lock();
		try {
			dbconfig.clear();
			searchconfig.clear();
			// TODO 判断ZKPATH的存在与否
			// throw new GDBException("zookeeper path : "+ZKPATH +"not exists");
			List<String> children = zkUtil.getChildren(ZKPATH, watch);
			if (children == null)
				return;
			for (String child : children) {
				// System.out.println(child);
				String[] type = child.split("-");
				Channel channel = null;
				try {
					channel = Channel.getChannel(Integer.parseInt(type[0]));
					if(channel==null)   continue;
					String value = zkUtil.readData(ZKPATH + "/" + child, watch);// 对每个children加watcher
					if (type[2].equals("db")) {
						dbconfig.put(channel, value);
					} else if (type[2].equals("search")) {
						searchconfig.put(channel, Integer.parseInt(value));
					}

				} catch (Exception e) {//容许格式出错,出错后不做处理,
					LOG.error(e.getMessage(), e);
					continue;
				}

			}
		} finally {
			w.unlock();
		}

	}
     
	/**
	 * 选择模式,并从zookeeper中加载配置
	 * @param mode
	 */
	public static void setMode(GDBMode mode) {
		AdaModeConfig.mode = mode;
		switch (mode) {
		case QUERY:
			ZKPATH = QUERY_CONFIG_PATH;
			watch = true;
			break;
		case INSERT:
			ZKPATH = INSERT_CONFIG_PATH;
			watch = false;
			break;
		default:
			break;
		}
       
        zkUtil.createConnection(CONNECTION_STRING, SESSION_TIMEOUT, new AdaModeConfigZKWatcher());
		zkUtil.exists(ZKPATH, watch);// 注册watcher.
		loadConfig();
	}

	public static String getDBVersion(Channel type) {
		r.lock();
		try {
			if (!dbconfig.containsKey(type))
				return "v1";
			return dbconfig.get(type);
		} finally {
			r.unlock();
		}
	}

	public static int getIndexNumber(Channel type) {
		r.lock();
		try {
			if (!searchconfig.containsKey(type))
				return type.getIntForm();
			return searchconfig.get(type);
		} finally {
			r.unlock();
		}
	}

	/**
	 * GDB库表存在两种模式:要么入库(STORE),要么查询(QUERY)
	 * 
	 */
	public static enum GDBMode {
		QUERY, INSERT
	}

	static class AdaModeConfigZKWatcher implements Watcher {
		private static final Logger LOG = LoggerFactory
				.getLogger(AdaModeConfigZKWatcher.class);
		public void process(WatchedEvent event) {
			// 连接状态
			KeeperState keeperState = event.getState();
			// 事件类型
			EventType eventType = event.getType();
			// 受影响的path
			String path = event.getPath();
			String logPrefix = "";
           
			LOG.info(logPrefix + "收到Watcher通知");
			LOG.info(logPrefix + "连接状态:\t" + keeperState.toString());
			LOG.info(logPrefix + "事件类型:\t" + eventType.toString());
  
			if (KeeperState.SyncConnected == keeperState) {
				// 成功连接上ZK服务器
			//	System.out.println("ddddddddddddddddd");
				if (EventType.None == eventType) {
					LOG.info(logPrefix + "成功连接上ZK服务器");
					ZKUtil.connectedSemaphore.countDown();
				} else if(EventType.NodeDeleted!=eventType){//NodeDeleted这种状态会引发父节点的EventType.NodeChildrenChanged,因此不做重复处理
					AdaModeConfig.loadConfig();//使用读写锁 来同步,
					LOG.info(logPrefix + "成功更新配置");
				}

			}else if (KeeperState.Expired == keeperState){//
				LOG.info(logPrefix + "与ZK连接超时，重新连接");//don't kown why?
				AdaModeConfig.setMode(mode);
			} 
			else if (KeeperState.Disconnected == keeperState) {
				LOG.info(logPrefix + "与ZK服务器断开连接");
			} else if (KeeperState.AuthFailed == keeperState) {
				LOG.info(logPrefix + "权限检查失败");
			} else if (KeeperState.Expired == keeperState) {
				LOG.info(logPrefix + "会话失效");
			}

			LOG.info("--------------------------------------------");

		}
	}
}
