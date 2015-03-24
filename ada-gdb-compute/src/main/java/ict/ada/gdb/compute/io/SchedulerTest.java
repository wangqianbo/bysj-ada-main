package ict.ada.gdb.compute.io;

import java.io.IOException;

import ict.ada.common.model.NodeType.Channel;
import ict.ada.gdb.common.AdaModeConfig;
import ict.ada.gdb.common.GdbException;
import ict.ada.gdb.service.AdaGdbService;

public class SchedulerTest {
	 public static final  AdaGdbService adaGdbService = new AdaGdbService(AdaModeConfig.GDBMode.QUERY);
	public static void main(String[] args) {
		try {
			adaGdbService.schedule(Long.parseLong(args[0]), Channel.WEIBO);
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (GdbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
