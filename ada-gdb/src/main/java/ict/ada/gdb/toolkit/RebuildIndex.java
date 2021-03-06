package ict.ada.gdb.toolkit;

import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.AdaConfig;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.AdaModeConfig.GDBMode;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.service.AdaGdbService;
import ict.ada.gdb.util.Pair;
import ict.ada.search.AdaSearchService;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class RebuildIndex {



	  /**
	   * Rebuild in a search channel.
	   * 
	   * @param type
	   */
	  public static void rebuildIndexedNames(GDBMode mode, Channel channel) {
		  AdaGdbService service = new AdaGdbService();
	    AdaModeConfig.setMode(mode);
	    short indexType = (short) AdaModeConfig.getIndexNumber(channel);
	    System.out.println("NOTICE: ");
	    System.out.println("You are requesting to REBUILD INDEX  in the search channel for " + channel
	        + " with indexType = " + indexType);
	    System.out.println("Make sure that no jobs are writing to the search system.");
	    System.out.println("Search system address: " + AdaConfig.INDEX_SERVER_ADDR + ", "
	        + AdaConfig.MQ_SERVER_ADDR + ", mqname: " + AdaConfig.MQ_NAME);
	    System.out.println("\nContinue? (Y/n)");
	    if (false == confirm()) {
	      System.out.println("Bye!");
	      return;
	    }

	    AdaSearchService searcher =
	        new AdaSearchService(AdaConfig.INDEX_SERVER_ADDR, AdaConfig.MQ_SERVER_ADDR,
	            AdaConfig.MQ_NAME);

	    Short v = (short) AdaModeConfig.getIndexNumber(channel);
	    if (v == null) {
	      throw new IllegalArgumentException("No mapping for " + channel.toString()
	          + " in search system.");
	    }
	    List<Pair<Channel, Short>> affectedType = new ArrayList<Pair<Channel, Short>>();
	    affectedType.add(new Pair<Channel,Short>(channel,v));
	    // some NodeTypes may share one search channel, so print the affected search channels here
	    System.out.println("\033[32mMODE:" + mode.name() + "\033[39m");
	    System.out.println("Affected Types:");
	    System.out.println("\t[Channel]\t[Search Channel]");
	    for (Pair<Channel, Short> e : affectedType) {
	      System.out.println("\t" + e.getFirst().toString() + "\t" + e.getSecond());
	    }

	    System.out
	        .println("\nWARNING! Are you sure to REBUILD INDEX in these channels? (Input 'Y' to start deletion)");
	    if (true == confirm()) {
	      System.out.println("Trying to REBUILD...");
	      boolean result = searcher.deleteIndex(indexType);
	      try {
			service.rebuildIndexByChannel(channel);
		} catch (GdbException e1) {
			e1.printStackTrace();
		}
	      System.out.println("result=" + result);
	    } else {
	      System.out.println("No data deleted.");
	      System.out.println("Bye!");
	    }
	  }

	  private static Scanner cin;

	  private static boolean confirm() {
	    if (cin == null) {
	      cin = new Scanner(System.in);
	    }
	    String answer = cin.nextLine().trim();
	    return answer.equals("Y");
	  }


	  private static void usage() {
	    System.out.println("\033[31mUsage: [mode]  ChannelString\033[39m");
	    System.out.println("mode is optional, value can be QUERY, INSERT, default is INSERT.");
	    System.out.println("Channel String list:");
	    for (Channel cl : Channel.values()) {
	      if (cl != Channel.ANY) {
	        System.out.print(cl + "  ");
	      }
	    }
	    System.out.println("\n");
	  }

	  private static void processSubCommands(String[] args) throws GdbException {
	    GDBMode mode = GDBMode.INSERT;
	    if (args.length == 2) {
	      if (args[0].equalsIgnoreCase(GDBMode.INSERT.name())) {
	        mode = GDBMode.INSERT;
	      } else if (args[0].equalsIgnoreCase(GDBMode.QUERY.name())) {
	        mode = GDBMode.QUERY;
	      } else {
	        System.err.println("\033[31mERROR: unknown MODE: " + args[0] + "\033[39m");
	        usage();
	        return;
	      }
	      args[0] = args[1];
	    }
	    try {
	    	RebuildIndex.rebuildIndexedNames(mode, Channel.valueOf(args[0]));
	    } finally {
	      if (cin != null) cin.close();
	    }
	  }

	  public static void main(String[] args) throws Exception {
	    if (args.length != 1 && args.length != 2) {
	      usage();
	      return;
	    }
	    processSubCommands(args);
	  }


}
