package ict.ada.gdb.rowcounter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.AdaConfig;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.dao.HBaseDAOFactory;
import ict.ada.gdb.dao.HBaseEdgeDAO;
import ict.ada.gdb.schema.RelationTypeHTable;

public class RelationTypeCounter {

	public static HBaseEdgeDAO edgeDao = HBaseDAOFactory.getHBaseEdgeDAO();

	public static HTableDescriptor genTable(String tableName,
			boolean bloomfilter) {
		HTableDescriptor table = new HTableDescriptor(tableName);
		HColumnDescriptor family = new HColumnDescriptor("i");
		if (bloomfilter) 
			family.setBloomFilterType(BloomType.ROW);
		family.setCompressionType(Compression.Algorithm.SNAPPY);
		table.addFamily(family);
		return table;
	}

	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException {
		AdaModeConfig.setMode(AdaModeConfig.GDBMode.QUERY);
		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("resource")
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.exit(-1);
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.exit(-1);
		}
		String channelNames = AdaConfig.RELATIONTYPE_CHANNELS;
	  String tableName = RelationTypeHTable.getName(Channel.KNOWLEDGE);
    try {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    admin.createTable(genTable(tableName, true));
    HTable table = null;
 try{   table = new HTable(conf, tableName);
		List<Channel> channels = new ArrayList<Channel>();
		if (channelNames == null)
			for (Channel channel : Channel.values())
				channels.add(channel);
		else {
			for (String channelName : channelNames.split(":"))
				channels.add(Channel.getChannel(Integer.parseInt(channelName)));
		}
		for (Channel channel : channels) {

			Map<String, byte[]> relationTypes = edgeDao
					.getRelationType(channel);
			// HTable table = new
			// HTable(conf,RelationTypeHTable.getName(channel));
		
			int len = 0;
			List<Put> putList = new ArrayList<Put>();
			for (Map.Entry<String, byte[]> e : relationTypes.entrySet()) {
				Put put = new Put(Bytes.add(channel.getByteFrom(), e.getKey().getBytes()));
				put.add(RelationTypeHTable.FAMILY,
						RelationTypeHTable.QUALIFIER, e.getValue());
				len++;
				putList.add(put);
				if (len == 1000) {

					table.batch(putList);

					putList.clear();
					len = 0;
				}
			}
			if (len != 0)
				table.batch(putList);

		}}finally{
		  table.close();
		}
	}

}
