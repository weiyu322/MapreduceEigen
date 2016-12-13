package com.myhadoop.matrixdecomposition.lanczos;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class LanczosTimer {

	public static HashMap<String,Long> START;
	public static boolean DEBUG;
	public static BufferedWriter bw;

	public static void init() throws IOException{
		START = new HashMap<String, Long>();
		DEBUG = true;
		File file = new File("/data/timer.out");
		if(file.exists()){
			file.delete();
		}
		file.createNewFile();
		bw = new BufferedWriter(new FileWriter(file));
		
	}
	
	public static void openOut(){
		DEBUG = true;
	}
	public static void closeOut(){
		DEBUG = false;
	}
	
	public static void close() throws IOException{
		bw.close();
	}
	
	public static void start(String str){
		START.put(str, System.nanoTime());
	}
	public static double end(String str) throws IOException{
		Long tmp = START.get(str);
		START.remove(str);
		double p = (System.nanoTime()-tmp)/1E9;
		if(DEBUG){
			System.out.println(str+":"+p);
			bw.write(str+":"+p);
			bw.newLine();
			bw.flush();
		}
		
		return p;
	}
}
