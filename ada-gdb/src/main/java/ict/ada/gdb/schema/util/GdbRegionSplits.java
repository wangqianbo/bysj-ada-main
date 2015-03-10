package ict.ada.gdb.schema.util;

import ict.ada.common.model.NodeType;
import ict.ada.common.model.NodeType.Attribute;
import ict.ada.common.model.NodeType.Channel;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.StringUtils;

public class GdbRegionSplits {
private static final String PREFIX = "\\x";
	public static String getSplitByteString(byte[] splitByte){
		
		String split = StringUtils.byteToHexString(splitByte).toUpperCase();
	    int i=0;
	    StringBuilder splitBytes=new StringBuilder();
	    while(i<split.length()){
	    	splitBytes.append(PREFIX);
	    	splitBytes.append(split.substring(i, i+2));
	    	i+=2;
	    }
	    return splitBytes.toString();
	}
	
	public static String getRowSplits(Channel channel,int regionNum,boolean salt){
		String startRow="";
		String endRow="";
		if(salt){
			startRow="00";
			endRow="1f";
		}
		else{
			String prefix=StringUtils.byteToHexString(NodeType.getType(channel, Attribute.PERSON).getBytesForm());
			startRow=prefix+"00";
			endRow=prefix+"ff";
		}
		byte[][] splits=CreateTable.split(startRow, endRow, regionNum);
		StringBuilder splitString=new StringBuilder();
		for(byte[] split:splits){
			splitString.append("\"").append(getSplitByteString(split)).append("\"").append(", ");
		}
		String splitString1=splitString.toString();
		splitString1=splitString1.substring(0,splitString1.length()-2);
        return ", {SPLITS => [ "+splitString1+" ]}";
	}
	
	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		Options opt = new Options();
		opt.addOption("h", false, "Print this usage help");
		CommandLine cmd = new GnuParser().parse(opt, args);
		if (cmd.hasOption("h")) {
			new HelpFormatter()
					.printHelp(
							"GdbRegionSplits <CHANNEL> <NUMREGIONS>\n"
									+ "CHANNEL is the channel's 8 tables you want to create.\n "
									+ "NUMREGIONS presplit regions \n"
									+ "RESULT : 1 for nodeId,nodeAttr,nodeWdeRefs,edgeId,relationWdeRefs , 2 for edgeRelWeightSum,edgeRelWeightDetail nodeName not support \n"
									, opt);
			return;
		}

		String channel = cmd.getArgs()[0];
		int numRegions=Integer.parseInt(cmd.getArgs()[1]);
		Channel channelType = CreateTable.map.get(channel);
		if (channelType == null) {
			System.out.println("wrong channel : " + channel);
			System.out.println("All Channels : " + CreateTable.map.toString());
			System.exit(-1);
		}
		System.out.println(1+":\t"+GdbRegionSplits.getRowSplits(channelType,numRegions,false));
		System.out.println(2+":\t"+GdbRegionSplits.getRowSplits(channelType,numRegions,true));
	}

}
