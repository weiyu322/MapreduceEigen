package com.myhadoop.matrixdecomposition.datamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class DistributedRowMatrix
{
	private int dimension;
	private Path inputPath;
	private Path outputTmpPath;
	
	public Path getPath(){
		return inputPath;
	}
	public Path getTmpPath(){
		return outputTmpPath;
	}
	public int getDim(){
		return dimension;
	}
	
	public DistributedRowMatrix(Path in,Path outTmp,int dim){
		this.dimension = dim;
		this.inputPath = in;
		this.outputTmpPath = outTmp;
	}
	

	public static class MatrixEntryWritable implements WritableComparable<MatrixEntryWritable> {
	    private int col;
	    private double val;



	    public int getCol() {
	      return col;
	    }

	    public void setCol(int col) {
	      this.col = col;
	    }

	    public double getVal() {
	      return val;
	    }

	    public void setVal(double val) {
	      this.val = val;
	    }

	    @Override
	    public int compareTo(MatrixEntryWritable o) {
	        if (col > o.col) {
	          return 1;
	        } else if (col < o.col) {
	          return -1;
	        } else {
	          return 0;
	        }
	    }

	    @Override
	    public boolean equals(Object o) {
	      if (!(o instanceof MatrixEntryWritable)) {
	        return false;
	      }
	      MatrixEntryWritable other = (MatrixEntryWritable) o;
	      return col == other.col;
	    }

	    @Override
	    public int hashCode() {
	      return col;
	    }

	    @Override
	    public void write(DataOutput out) throws IOException {
	      out.writeInt(col);
	      out.writeDouble(val);
	    }

	    @Override
	    public void readFields(DataInput in) throws IOException {
	      col = in.readInt();
	      val = in.readDouble();
	    }

	    @Override
	    public String toString() {
	      return  col + ":" + val;
	    }
	  }



}