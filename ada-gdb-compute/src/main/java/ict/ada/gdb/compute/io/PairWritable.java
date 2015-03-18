package ict.ada.gdb.compute.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PairWritable<T1 extends Writable, T2 extends Writable> implements Writable{
	private T1 value1;
	private T2 value2;
	@Override
	public void readFields(DataInput input) throws IOException {
		value1.readFields(input);
		value2.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		value1.write(output);
		value2.write(output);
	}

	public T1 getValue1() {
		return value1;
	}

	public void setValue1(T1 value1) {
		this.value1 = value1;
	}

	public T2 getValue2() {
		return value2;
	}

	public void setValue2(T2 value2) {
		this.value2 = value2;
	}
	
}
