package com.myhadoop.matrixdecomposition.datamodel;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

//向量类，封装了double型数组
public class ArrayDoubleWritable implements Writable
{
	private int size;
	private double[] vector;

	public ArrayDoubleWritable(){
		
	}
	
	public int getSize(){
		return size;
	}
	public double getValue(int i){
		return vector[i];
	}
	public void setValue(int i,double value){
		vector[i] = value;
	}
	public ArrayDoubleWritable(int size){
		this.size = size;
		this.vector = new double[size];
	}

	public double getSum(){
		double sum=0.0;
		for(int i=0; i<size; i++){
			sum+=vector[i];
		}
		return sum;
	}
	public double norm2(){
		double sum=0.0;
		for(int i=0; i<size; i++){
			sum+=Math.pow(vector[i], 2);
		}
		
		return Math.sqrt(sum);
	}
	public void normalize(){
		double norm2 = norm2();
		for(int i=0; i<size; i++){
			double after = vector[i]/norm2;
			vector[i] = after;
		}
	}
	
	
	public double dotVector(ArrayDoubleWritable v){
		double value=0;
		for(int i=0; i<size; i++){
			value+=vector[i]*v.getValue(i);
		}
		return value;
	}
	
	public void plusVector(ArrayDoubleWritable v){
		double afterPlus;
		for(int i=0; i<size; i++){
			afterPlus = vector[i]+v.getValue(i);
			vector[i] = afterPlus;
		}
	}
	public void mulConst(double c){
		double oldValue;
		for(int i=0; i<size; i++){
			oldValue=vector[i];
			vector[i] = oldValue*c;
		}
	}
	public void plusAlphaMulVector(ArrayDoubleWritable v,double alpha){
		double afterPlus;
		for(int i=0; i<size; i++){
			afterPlus = vector[i]+v.getValue(i)*alpha;
			vector[i] = afterPlus;
		}
	}
	
	public static void writeToCache(Path dest,Configuration conf,ArrayDoubleWritable m) throws IOException, URISyntaxException{
		FileSystem fs = FileSystem.get(dest.toUri(), conf);
		dest = fs.makeQualified(dest);
		DistributedCache.setCacheFiles(new URI[]{dest.toUri()}, conf);
		
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, dest, NullWritable.class, ArrayDoubleWritable.class);
		writer.append(NullWritable.get(), m);
		writer.close();
//		DistributedCache.addCacheFile(new URI("/lanczos/mul/myDistributedCache.java"),conf);
		
	}
	
	public static ArrayDoubleWritable loadFromCache(Configuration conf) throws IOException{
		URI[] files = DistributedCache.getCacheFiles(conf);
		Path data = new Path(files[0].getPath());
		
	    FileSystem fs = FileSystem.get(conf);

	    data.makeQualified(fs);
	    SequenceFile.Reader reader = new SequenceFile.Reader(fs, data, conf);
	    Class<? extends Writable> keyClass = (Class<? extends Writable>) reader.getKeyClass();
	    Writable key = ReflectionUtils.newInstance(keyClass, conf);
//	    Class<HashMapWritable> valueClass = (Class<HashMapWritable>) reader.getValueClass();
	    
	    ArrayDoubleWritable value = new ArrayDoubleWritable();
	    
	    reader.next(key, value);

	    reader.close();
		return value;
		
	}
	
	public static ArrayDoubleWritable loadFromHDFS(Configuration conf, Path out, int dim) 
	throws IOException{
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(out);
		ArrayDoubleWritable m = new ArrayDoubleWritable(dim);
		NullWritable key = NullWritable.get();
		IntDoublePairWritable value = new IntDoublePairWritable();
		for(int i=0; i<stats.length; i++)
		{
			//if stats[i] is a file
			if(stats[i].isFile()){
				//System.out.println("error when:" + stats[i].getPath());
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, stats[i].getPath(), conf);
				while((reader.next(key, value)) == true){
					m.setValue(value.getKey(),value.getValue());
				}
				reader.close();
			}
		}
		return m;
	}
	
	public String print(){
		StringBuilder sb = new StringBuilder();
		for(int i=0; i<size; i++){
			sb.append(i+":"+vector[i]+" ");
		}
		return sb.toString();
	}
	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		out.writeInt(size);
		for(int i=0; i<size; i++){
			out.writeDouble(vector[i]);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		size = in.readInt();
		vector = new double[size];
		for(int i=0; i<size; i++){
			vector[i]=in.readDouble();
		}
	}
	

}
