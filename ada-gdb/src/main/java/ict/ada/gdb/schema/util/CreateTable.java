package ict.ada.gdb.schema.util;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.schema.GdbHTableConstant;
import ict.ada.gdb.schema.GdbHTablePartitionPolicy;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

public class CreateTable {

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
	public static void main(String[] args) throws IOException, ParseException {

		// parse user input
		Options opt = new Options();
		opt.addOption(OptionBuilder
				.withArgName("tableName=regions")
				.hasArg()
				.withDescription(
						"split the table:"
								+ "tableName[nodeId,nodeAttr,nodeWdeRefs,edgeId,edgeRelWeightSum,edgeRelWeightDetail,relationWdeRefs]"
								+ "=regions[number of regions]").create("s"));
		opt.addOption("h", false, "Print this usage help");

		CommandLine cmd = new GnuParser().parse(opt, args);

		if (cmd.hasOption("h")) {
			new HelpFormatter()
					.printHelp(
							"CreateTable <CHANNEL> <-s TABLENAME=NUMREGIONS>\n"
									+ "CHANNEL is the channel's 8 tables you want to create.\n "
									+ "TABLENAME,  nodeId,nodeAttr,nodeWdeRefs,edgeId,edgeRelWeightSum,edgeRelWeightDetail,relationWdeRefs are tables support presplit \n"
									+ "NUMREGIONS presplit regions ", opt);
			return;
		}

		String channel = cmd.getArgs()[0];
		Channel channelType = map.get(channel);
		if (channelType == null) {
			System.out.println("wrong channel : " + channel);
			System.out.println("All Channels : " + map.toString());
			System.exit(-1);
		}
		AdaModeConfig.setMode(AdaModeConfig.GDBMode.INSERT);
		Set<String> tableNames = GdbHTablePartitionPolicy
				.generateHTableNames(new Channel[] { channelType });
		String type = StringUtils.byteToHexString(NodeType.getType(channelType,
				Attribute.PERSON).getBytesForm());
		String startRow = type + "00";
		String endRow = type + "ff";
		HashMap<String, Integer> splitTables = new HashMap<String, Integer>();
		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("resource")
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (cmd.hasOption("s")) {
			for (String confOpt : cmd.getOptionValues("s")) {
				String[] kv = confOpt.split("=", 2);
				if (kv.length == 2) {
					int numSpits = Integer.parseInt(kv[1]);
					String tableName = GdbHTableConstant.SHARED_PREFIX + "-"
							+ kv[0] + "-" + channel;
					if (numSpits > 1) {
						if (tableNames.contains(tableName))
							splitTables.put(tableName, numSpits);
					}

				} else {
					throw new ParseException("-s option format invalid: "
							+ confOpt);
				}
			}
		}

		for (String tableName : tableNames) {
			boolean bloomfilter = false;
			String startkey = null;
			String endKey = null;
			int regions = 1;
			if (tableName.startsWith(GdbHTableConstant.SHARED_PREFIX + "-"
					+ "nodeId")
					|| tableName.startsWith(GdbHTableConstant.SHARED_PREFIX
							+ "-" + "edgeId")||tableName.startsWith(GdbHTableConstant.SHARED_PREFIX
									+ "-" + "nodeName"))
				bloomfilter = true;
			if (splitTables.containsKey(tableName)) {
				regions = splitTables.get(tableName);
				if (tableName.startsWith(GdbHTableConstant.SHARED_PREFIX + "-"
						+ "edgeRelWeightSum")
						|| tableName.startsWith(GdbHTableConstant.SHARED_PREFIX
								+ "-" + "edgeRelWeightDetail")) {
					startkey = "00";
					endKey = "1f";
				} else {
					startkey = startRow;
					endKey = endRow;
				}

			}
			if (regions > 1) {
				admin.createTable(genTable(tableName, bloomfilter),
						split(startkey, endKey, regions));
			} else
				admin.createTable(genTable(tableName, bloomfilter));
		}

	}
}
