package ict.ada.gdb.compute.io;

import org.apache.hadoop.hbase.util.Bytes;
import org.javatuples.Pair;

public class ValueDecoderUtil {

	public static byte[] encode(float v1,float v2){
		byte[] result = new byte[8];
		byte[] v1ToBytes = Bytes.toBytes(v1);
		byte[] v2ToBytes = Bytes.toBytes(v2);
		for(int i=0;i<4;i++){
			result[i] = v1ToBytes[i];
			result[i+4] = v2ToBytes[i];
		}
		return result;
	}
	public static Pair<Float,Float> decode(byte[] value){
		if(value.length!=8) throw new IllegalArgumentException();
		return  new Pair<Float,Float>(Bytes.toFloat(value, 0),Bytes.toFloat(value,4));
		
	}
	public static void main(String[] args) {
		Pair<Float,Float> result = ValueDecoderUtil.decode(ValueDecoderUtil.encode(343242, -2312));
		System.out.println(result.getValue0());
		System.out.println(result.getValue1());
	}

}
