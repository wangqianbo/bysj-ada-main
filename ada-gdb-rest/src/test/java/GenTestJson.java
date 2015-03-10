import ict.ada.gdb.rest.util.PojoMapper;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.codehaus.jackson.annotate.JsonProperty;

public class GenTestJson {
	@JsonProperty("addl")
	  public List<String> additional;



  public static void main(String[] args) throws Exception {
    // Random r1 = new Random(10);
	  GenTestJson bean = (GenTestJson) PojoMapper.fromJson("{\"addl\":\"test\"}", GenTestJson.class);
	  System.out.println(bean.additional);


  }
}