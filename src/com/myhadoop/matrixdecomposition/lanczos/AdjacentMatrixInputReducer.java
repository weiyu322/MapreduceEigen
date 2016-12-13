package com.myhadoop.matrixdecomposition.lanczos;

import java.io.IOException;
import java.util.HashSet;

import com.myhadoop.matrixdecomposition.datamodel.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AdjacentMatrixInputReducer
extends Reducer<IntWritable,IntWritable,IntWritable,HashMapWritable>{

	@Override
	protected void reduce(IntWritable key,Iterable<IntWritable> values,Context context)
	throws IOException,InterruptedException
	{
		HashMapWritable m = new HashMapWritable();
		for (IntWritable value:values)
		{
			m.add(value.get(), 1);
		}
		context.write(key, m);
	}
	
	
}