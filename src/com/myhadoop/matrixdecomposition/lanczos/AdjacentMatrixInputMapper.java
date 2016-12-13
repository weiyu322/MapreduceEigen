package com.myhadoop.matrixdecomposition.lanczos;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AdjacentMatrixInputMapper
extends Mapper<LongWritable,Text,IntWritable,IntWritable>
{
	
	@Override
	protected void map(LongWritable key,Text value,Context context)
	throws IOException,InterruptedException
	{
		String line = value.toString();
		String[] content = line.split(",");
		
		if(content.length != 2){
			throw new IOException("Input error"+value.toString());
		}
		IntWritable row = new IntWritable(Integer.parseInt(content[0]));
		IntWritable col = new IntWritable(Integer.parseInt(content[1]));
		context.write(row, col);
		//context.write(col, row);
	}
}