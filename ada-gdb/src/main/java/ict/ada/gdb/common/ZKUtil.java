package ict.ada.gdb.common;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKUtil {

	private static final Logger LOG = LoggerFactory.getLogger(ZKUtil.class);
	private static final String LOG_PREFIX_OF_MAIN = "【Main】";
	public static CountDownLatch connectedSemaphore = new CountDownLatch(1);
	private ZooKeeper zk = null;

	/**
	 * 创建ZK连接
	 * 
	 * @param connectString
	 *            ZK服务器地址列表
	 * @param sessionTimeout
	 *            Session超时时间
	 */
	public void createConnection(String connectString, int sessionTimeout,
			 Watcher watcher) {
		this.releaseConnection();

		try {
			zk = new ZooKeeper(connectString, sessionTimeout, watcher);
			LOG.info(LOG_PREFIX_OF_MAIN + "开始连接ZK服务器");
			connectedSemaphore.await();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 关闭ZK连接
	 */
	public void releaseConnection() {
		if (zk != null)
			try {
				this.zk.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}

	/**
	 * 创建节点
	 * 
	 * @param path
	 *            节点path
	 * @param data
	 *            初始数据内容
	 * @return
	 */
	public boolean createPath(String path, String data) {
		try {
			this.zk.exists(path, true);
			LOG.info(LOG_PREFIX_OF_MAIN + "节点创建成功, Path: "
					+ this.zk.create(path, //
							data.getBytes(), //
							Ids.OPEN_ACL_UNSAFE, //
							CreateMode.PERSISTENT) + ", content: " + data);
		} catch (Exception e) {
		}
		return true;
	}

	/**
	 * 读取指定节点数据内容
	 * 
	 * @param path
	 *            节点path
	 * @return
	 */
	public String readData(String path, boolean needWatch) {
		try {
			return new String(this.zk.getData(path, needWatch, null));
		} catch (Exception e) {
			return "";
		}
	}

	/**
	 * 更新指定节点数据内容
	 * 
	 * @param path
	 *            节点path
	 * @param data
	 *            数据内容
	 * @return
	 */
	public boolean writeData(String path, String data) {
		try {
			LOG.info(LOG_PREFIX_OF_MAIN + "更新数据成功，path：" + path + ", stat: "
					+ this.zk.setData(path, data.getBytes(), -1));
		} catch (Exception e) {
		}
		return false;
	}

	/**
	 * 删除指定节点
	 * 
	 * @param path
	 *            节点path
	 */
	public void deleteNode(String path) {
		try {
			this.zk.delete(path, -1);
			LOG.info(LOG_PREFIX_OF_MAIN + "删除节点成功，path：" + path);
		} catch (Exception e) {
			// TODO
		}
	}

	/**
	 * 删除指定节点
	 * 
	 * @param path
	 *            节点path
	 */
	public Stat exists(String path, boolean needWatch) {
		try {
			return this.zk.exists(path, needWatch);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * 获取子节点
	 * 
	 * @param path
	 *            节点path
	 */
	public List<String> getChildren(String path, boolean needWatch) {
		try {
			return this.zk.getChildren(path, needWatch);
		} catch (Exception e) {
			return null;
		}
	}
public ZooKeeper getZK(){
  return zk;
}
}
