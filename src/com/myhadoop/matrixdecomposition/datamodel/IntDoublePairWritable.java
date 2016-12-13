package com.myhadoop.matrixdecomposition.datamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class IntDoublePairWritable implements Writable {
	  
	  private int key;
	  private double value;
	  
	  public IntDoublePairWritable() {
	  }
	  
	  public IntDoublePairWritable(int k, double v) {
	    this.key = k;
	    this.value = v;
	  }
	  
	  public void setKey(int k) {
	    this.key = k;
	  }
	  
	  public void setValue(double v) {
	    this.value = v;
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    this.key = in.readInt();
	    this.value = in.readDouble();
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    out.writeInt(key);
	    out.writeDouble(value);
	  }

	  public int getKey() {
	    return key;
	  }

	  public double getValue() {
	    return value;
	  }

	  public String toString(){
		  return new String(key+":"+value);
		  
	  }
	}
