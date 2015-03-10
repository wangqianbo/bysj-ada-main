package ict.ada.gdb.schema.util;

import ict.ada.common.model.Edge;
import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.schema.GdbHTableConstant;
import ict.ada.gdb.schema.GdbHTablePartitionPolicy;
import ict.ada.gdb.schema.RelationTypeHTable;
import ict.ada.gdb.service.AdaGdbService;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

public class ScanForRelationType {

	public static HashMap<String, Channel> map = new HashMap<String, Channel>();

	static {
		map.put("linkedin", Channel.LINKEDIN);
		map.put("web", Channel.WEB);
		map.put("blog", Channel.BLOG);
		map.put("baidubaike", Channel.BAIDUBAIKE);
		map.put("hudongbaike", Channel.HUDONGBAIKE);
		map.put("wikibaike", Channel.WIKIBAIKE);
		map.put("BBS", Channel.BBS);
		map.put("news", Channel.NEWS);
		map.put("scholar", Channel.SCHOLAR);
		map.put("twitter", Channel.TWITTER);
		map.put("weibo", Channel.WEIBO);
		map.put("any", Channel.ANY);
	}

	public static boolean createTable(HBaseAdmin admin, HTableDescriptor table,
			byte[][] splits) throws IOException {
		try {
			admin.createTable(table, splits);
			return true;
		} catch (TableExistsException e) {
			// logger.info("table " + table.getNameAsString() +
			// " already exists");
			// the table already exists...
			return false;
		}
	}

	public static byte[][] getHexSplits(String startKey, String endKey,
			int numRegions) {
		byte[][] splits = new byte[numRegions - 1][];
		BigInteger lowestKey = new BigInteger(startKey, 16);
		BigInteger highestKey = new BigInteger(endKey, 16);
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger
				.valueOf(numRegions));
		lowestKey = lowestKey.add(regionIncrement);
		for (int i = 0; i < numRegions - 1; i++) {
			BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger
					.valueOf(i)));
			byte[] b = String.format("%016x", key).getBytes();
			splits[i] = b;
		}
		return splits;
	}

	public static byte[][] split(String startKey, String endKey, int numRegions) {
		byte[] firstRowBytes = StringUtils.hexStringToByte(startKey);
		byte[] lastRowBytes = StringUtils.hexStringToByte(endKey);
		Preconditions.checkArgument(
				Bytes.compareTo(lastRowBytes, firstRowBytes) > 0,
				"last row (%s) is configured less than first row (%s)",
				Bytes.toStringBinary(lastRowBytes),
				Bytes.toStringBinary(firstRowBytes));

		byte[][] splits = Bytes.split(firstRowBytes, lastRowBytes, true,
				numRegions - 1);
		Preconditions.checkState(splits != null,
				"Could not split region with given user input: ");

		// remove endpoints, which are included in the splits list
		return Arrays.copyOfRange(splits, 1, splits.length - 1);
	}

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

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws IOException, ParseException, GdbException {

		// parse user input
		Options opt = new Options();
		opt.addOption(OptionBuilder
				.withArgName("channels")
				.hasArg()
				.withDescription(
						"scan the channel:" + "channels[weibo,web,linkedin...]")
				.create("c"));
		opt.addOption(OptionBuilder
				.withArgName("mode")
				.hasArg()
				.withDescription(
						"the mode of the channel:" + "mode[insert,query]")
				.create("m"));
		opt.addOption("h", false, "Print this usage help");

		CommandLine cmd = new GnuParser().parse(opt, args);

		if (cmd.hasOption("h")) {
			new HelpFormatter().printHelp(
					"\nScanForRelationType <MODE> <-c CHANNELS>\n"
							+ "MODE:insert or query\n "
							+ "CHANNELS,  the channels to scan", opt);
			return;
		}
		String mode = cmd.getArgs()[0];
		AdaGdbService adaGdbService = null;
		if (mode.equals("insert")) {
			adaGdbService = new AdaGdbService(AdaModeConfig.GDBMode.INSERT);
			AdaModeConfig.setMode(AdaModeConfig.GDBMode.INSERT);
		} else {
			adaGdbService = new AdaGdbService(AdaModeConfig.GDBMode.QUERY);
			AdaModeConfig.setMode(AdaModeConfig.GDBMode.QUERY);
		}
		HashSet<Channel> channelsForScan = new HashSet<Channel>();
		if (cmd.hasOption("c")) {
			String channels = cmd.getOptionValue("c");
			String[] channelArray = channels.split(",");

			for (String channelName : channelArray) {
				Channel channel = map.get(channelName);
				if (channel == null) {
					System.err.println("Wrong channel name: " + channelName);
					System.exit(-1);
				} else {
					if (channel == Channel.ANY) {
						for (Channel channel1 : Channel.values()) {
							if (channel1 != Channel.ANY)
								channelsForScan.add(channel1);
						}
					} else {
						channelsForScan.add(channel);
					}
				}
			}
		}
		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("resource")
		HBaseAdmin admin = new HBaseAdmin(conf);
		for (Channel channel2Scan : channelsForScan) {
			System.out.println("start to scan the channel " + channel2Scan);
			long time1 = System.currentTimeMillis();
			Map<String, byte[]> result = adaGdbService
					.getRelationType(channel2Scan);
			long time2 = System.currentTimeMillis();
			System.out.println("complete scan the channel " + channel2Scan
					+ " in " + (time2 - time1) + "ms");
			if (result != null && result.size() != 0) {
				String tableName = RelationTypeHTable.getName(channel2Scan);
				if (!admin.tableExists(tableName)) {
					admin.createTable(genTable(tableName, true));
				}
				HTable table = new HTable(conf, tableName);
				for (Map.Entry<String, byte[]> e : result.entrySet()) {
					Put put = new Put(e.getKey().getBytes());
					put.add(RelationTypeHTable.FAMILY,
							RelationTypeHTable.QUALIFIER,
							Arrays.copyOf(e.getValue(), Edge.EDGEID_SIZE));
					table.put(put);
				}
				table.close();
			}
		}
	}
}
