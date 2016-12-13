package com.myhadoop.matrixdecomposition.lanczos;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.myhadoop.matrixdecomposition.datamodel.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class AdjacentMatrixInputJob 
{

	/**
	 * @param args
	 */
	private AdjacentMatrixInputJob()
	{
			
	}
	
	//in:输入文件在HDFS上的路径
	//out:输出文件在HDFS上的路径
	public static void runJob(Path in,Path out) 
	throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException
	{
		Configuration conf = new Configuration();
			
		Job job = new Job(conf,"AdjacentMatrixInputJob");
			
			
		//FilsSystem是抽象类，需用get接口返回具体类
		//删除已有的输出文件夹
		FileSystem fs = FileSystem.get(conf);	
		fs.delete(out, true);
		fs.close();
			
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(HashMapWritable.class);
			
		//输出格式为序列化的对象
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
		job.setMapperClass(AdjacentMatrixInputMapper.class);
		job.setReducerClass(AdjacentMatrixInputReducer.class);
			
		job.setNumReduceTasks(40);
			
		//设置输入输出路径
		FileInputFormat.addInputPath(job, in);			
		FileOutputFormat.setOutputPath(job, out);
		    
		job.setJarByClass(AdjacentMatrixInputJob.class);
	
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) 
		{
			throw new IllegalStateException("Job failed!");
		}
		    
	}

}
