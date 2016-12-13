package com.myhadoop.matrixdecomposition.datamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/*
 * 稀疏向量
 */
public class HashMapWritable implements Writable
{
	private int size;
	private HashMap<Integer, Double> pair;		//稀疏向量
	
	public HashMapWritable(){
		size = 0;
		pair = new HashMap<Integer, Double>();
	}
	public HashMap<Integer, Double> getPair() {
		return pair;
	}
	public int getSize(){
		return size;
	}
	public Set<Integer> getKeySet(){
		return pair.keySet();
	}
	public double getValue(int i){
		return pair.get(i);
	}
	public void setValue(int i,double value){
		pair.put(i, value);
	}
	public void add(int col,double value){
		if(!pair.keySet().contains(col)){
			size++;
		}
		pair.put(col, value);
	}
	
	public int getMaxLabel(){
		int max=-1;
		for(Integer tmp : pair.keySet()){
			if(max < tmp){
				max = tmp;
			}
		}
		return max;
	}
	public double getSum(){
		double sum=0.0;
		for(Integer tmp:pair.keySet()){
			sum+=pair.get(tmp);
		}
		return sum;
	}
	public double norm2(){
		double sum=0.0;
		for(Integer tmp:pair.keySet()){
			sum+=Math.pow(pair.get(tmp), 2);
		}
		
		return Math.sqrt(sum);
	}
	public void normalize(){
		double norm2 = norm2();
		for(Integer tmp:pair.keySet()){
			double after = pair.get(tmp)/norm2;
			pair.put(tmp, after);
		}
	}
	
	
	public double dotVector(HashMapWritable v){
		double value=0;
		Set<Integer> vKeySet = v.pair.keySet();
		for(Integer tmp:pair.keySet()){
			if(vKeySet.contains(tmp)){
				value += pair.get(tmp)*(v.pair.get(tmp)); 
			}
		}
		return value;
	}
	public double dotArray(ArrayDoubleWritable v){
		double value=0;
		Set<Integer> vKeySet = pair.keySet();
		for(Integer tmp:vKeySet){
			value+=pair.get(tmp)*v.getValue(tmp);
		}
		return value;
	}
	
	public void plusVector(HashMapWritable v){
		double afterPlus;
		Set<Integer> vKeySet = v.pair.keySet();
		Set<Integer> keySet = pair.keySet();
		for(Integer tmp:vKeySet){
			if(keySet.contains(tmp)){
				afterPlus = pair.get(tmp)+v.pair.get(tmp); 
				pair.put(tmp, afterPlus);
			}
			else{
				afterPlus = v.pair.get(tmp);
				add(tmp, afterPlus);
			}
		}
	}
	public void mulConst(double c){
		double oldValue;
		for(Integer tmp:pair.keySet()){
			oldValue=pair.get(tmp);
			pair.put(tmp, oldValue*c);
		}
	}
	public void plusAlphaMulVector(HashMapWritable v,double alpha){
		double afterPlus;
		Set<Integer> vKeySet = v.pair.keySet();
		Set<Integer> keySet = pair.keySet();
		for(Integer tmp:vKeySet){
			if(keySet.contains(tmp)){
				afterPlus = pair.get(tmp)+alpha*(v.pair.get(tmp)); 
				pair.put(tmp, afterPlus);
			}
			else{
				afterPlus =alpha*(v.pair.get(tmp));
				add(tmp, afterPlus);
			}
		}
	}
	
	public static void writeToCache(Path dest,Configuration conf,HashMapWritable m) throws IOException, URISyntaxException{
		FileSystem fs = FileSystem.get(dest.toUri(), conf);
		dest = fs.makeQualified(dest);
		DistributedCache.setCacheFiles(new URI[]{dest.toUri()}, conf);
		
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, dest, NullWritable.class, HashMapWritable.class);
		writer.append(NullWritable.get(), m);
		writer.close();
//		DistributedCache.addCacheFile(new URI("/lanczos/mul/myDistributedCache.java"),conf);
		
	}
	public static HashMapWritable loadFromCache(Configuration conf) throws IOException{
		URI[] files = DistributedCache.getCacheFiles(conf);
		Path data = new Path(files[0].getPath());
		
	    FileSystem fs = FileSystem.get(conf);

	    data.makeQualified(fs);
	    SequenceFile.Reader reader = new SequenceFile.Reader(fs, data, conf);
	    Class<? extends Writable> keyClass = (Class<? extends Writable>) reader.getKeyClass();
	    Writable key = ReflectionUtils.newInstance(keyClass, conf);
	    Class<HashMapWritable> valueClass = (Class<HashMapWritable>) reader.getValueClass();
	    
	    HashMapWritable value = new HashMapWritable();
	    
	    reader.next(key, value);

	    reader.close();
		return value;
		
	}
	public static HashMapWritable loadFromHDFS(Configuration conf,Path out) 
	throws IOException{
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(out);
		HashMapWritable m = new HashMapWritable();
		NullWritable key = NullWritable.get();
		IntDoublePairWritable value = new IntDoublePairWritable();
		for(int i=0; i<stats.length; i++){
			if(!stats[i].isDir()){
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, stats[i].getPath(), conf);
				while((reader.next(key, value)) == true){
					m.add(value.getKey(),value.getValue());
				}
			}
		}
		return m;
	}
	
	public String print(){
		StringBuilder sb = new StringBuilder();
		for(Integer tmp:pair.keySet()){
			sb.append(tmp+":"+pair.get(tmp)+" ");
		}
		return sb.toString();
	}
	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		out.writeInt(size);
		for(Integer tmp:pair.keySet()){
			out.writeInt(tmp);
			out.writeDouble(pair.get(tmp));
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		int rsize = in.readInt();
		pair.clear();
		
		size=rsize;
		int ctmp;
		double vtmp;
		for(int i=0;i<rsize;i++){
			ctmp = in.readInt();
			vtmp = in.readDouble();
			pair.put(ctmp, vtmp);
		}
	}

}
