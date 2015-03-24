package ict.ada.gdb.compute.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class FloatFloatPairWritable implements Writable {
	private FloatWritable v1, v2;

	public FloatFloatPairWritable() {
		v1 = new FloatWritable();
		v2 = new FloatWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		v1.write(out);
		v2.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		v1.readFields(in);
		v2.readFields(in);
	}

	public FloatWritable getV1() {
		return v1;
	}

	public void setV1(FloatWritable v1) {
		this.v1 = v1;
	}

	public FloatWritable getV2() {
		return v2;
	}

	public void setV2(FloatWritable v2) {
		this.v2 = v2;
	}
	
}
