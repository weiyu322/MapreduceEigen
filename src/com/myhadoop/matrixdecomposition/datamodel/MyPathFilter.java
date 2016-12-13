package com.myhadoop.matrixdecomposition.datamodel;

import java.io.IOException;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.conf.Configuration;


public class MyPathFilter implements PathFilter 
{
	static String[] paths;
	static FileSystem fs;
	static Configuration conf;
	
	public static void setpaths(String[] _paths){
		paths=_paths;
	}
	public static void setConf(Object object){
		conf=(Configuration) object;
	}
	@Override
	public boolean accept(Path path) {
		// TODO Auto-generated method stub
		if((paths[0].equals("all"))||(paths[0].equals(""))){
			return true;
		}
		FileStatus fstatus=null;
		try {
			fs=FileSystem.get(conf);
			fstatus=fs.getFileStatus(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(!fstatus.isDir()){
			boolean result=false;
			for(int i=0;i<paths.length;i++){
				if(path.getName().indexOf(paths[i])!=-1){
					System.out.println(path.getName());
					result=true;
					break;
				}else{
					result=false;
				}
			}
			return result;
			
		}
		
		return true;
	}
}
