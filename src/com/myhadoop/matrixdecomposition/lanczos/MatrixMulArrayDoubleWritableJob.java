package com.myhadoop.matrixdecomposition.lanczos;

import java.io.IOException;
import java.net.URISyntaxException;

import com.myhadoop.matrixdecomposition.datamodel.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class MatrixMulArrayDoubleWritableJob {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	public MatrixMulArrayDoubleWritableJob()
	{
		
	}
	
	public static ArrayDoubleWritable runJob(DistributedRowMatrix d,ArrayDoubleWritable v) 
			throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException{
				
				Configuration conf = new Configuration();
				conf.set("mapred.child.java.opts", "-Xmx500M");
				Path mpath = new Path(d.getTmpPath(), "globalVector");
				Path out = new Path(d.getTmpPath(), "resultVector");
				FileSystem fs = FileSystem.get(conf);
				fs.delete(mpath, true);
				fs.delete(out,true);
				ArrayDoubleWritable.writeToCache(mpath, conf, v);
				
				Job job = new Job(conf,"MatrixMulArrayDoubleWritableJob");
				
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setOutputKeyClass(NullWritable.class);
				job.setOutputValueClass(IntDoublePairWritable.class);
				job.setOutputFormatClass(SequenceFileOutputFormat.class);
				
				job.setMapperClass(MatrixMulArrayMapper.class);
//				job.setReducerClass(MatrixMulArrayReducer.class);
				job.setNumReduceTasks(0);
				
			    FileInputFormat.addInputPath(job, d.getPath());
			    FileOutputFormat.setOutputPath(job, out);
				
				job.setJarByClass(MatrixMulArrayDoubleWritableJob.class);
				
			    boolean succeeded = job.waitForCompletion(true);
			    if (!succeeded) {
			      throw new IllegalStateException("MatrixMulArrayDoubleWritableJob failed!");
			    }
				
			    Path suc = new Path(out,"_SUCCESS");
			    fs.delete(suc,true);
			    
			    ArrayDoubleWritable m = ArrayDoubleWritable.loadFromHDFS(conf, out, d.getDim());
			    
				return m;
			}
			
			
			public static class MatrixMulArrayMapper
			extends Mapper<IntWritable,HashMapWritable,NullWritable,IntDoublePairWritable>{
				
				private ArrayDoubleWritable m;
				
				@Override
				protected void setup(Context context)
				throws IOException,InterruptedException{
				      super.setup(context);
				      Configuration config = context.getConfiguration();
				      m = ArrayDoubleWritable.loadFromCache(config);
//				      System.out.println(m.getKeySet().size());
				      if (m == null) {
				        throw new IOException("No vector loaded from cache!");
				      }
				      
				}
				
				@Override
				protected void map(IntWritable key,HashMapWritable value,Context context)
				throws IOException,InterruptedException{			
					double d = value.dotArray(m);
					IntDoublePairWritable pair = new IntDoublePairWritable(key.get(), d);
					context.write(NullWritable.get(), pair);
				}
				
			}
			
		

}
