package com.myhadoop.matrixdecomposition.lanczos;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.myhadoop.matrixdecomposition.datamodel.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;


//lanczos迭代的状态类

public class LanczosStatus
{
	protected DistributedRowMatrix matrix;
	protected double[][] triDiagonal;
	protected Map<Integer,ArrayDoubleWritable> iterVector;	 //存储迭代过程中的中间向量q_k
	protected Map<Integer,ArrayDoubleWritable> eigenVector;		//存储特征向量
	protected int desireRank;				//三对角阵的秩
	protected int iterationNumber;
	
	public LanczosStatus(DistributedRowMatrix m,int dR) throws IOException{
		matrix = m;
		desireRank = dR;
	    
		triDiagonal = new double[desireRank][desireRank];
		for (int i=0;i<desireRank;i++)
		{
			for (int j=0;j<desireRank;j++)
			{
				triDiagonal[i][j] = 0;
			}
		}
		iterVector = new HashMap<Integer, ArrayDoubleWritable>();
		ArrayDoubleWritable initial = new ArrayDoubleWritable(m.getDim());	
//		double value = 1/Math.sqrt(m.getDim());
		for(int i=0;i<m.getDim();i++){
			initial.setValue(i, 0.0);
		}
		setIterVector(0,initial);		//q_0
		
		for (int i=0;i<m.getDim();i++)
		{
			initial.setValue(i,(double)(Math.random()*10));
		}
		initial.normalize();

		setIterVector(1, initial);	    //q_1
		eigenVector = new HashMap<Integer, ArrayDoubleWritable>();
		iterationNumber = 1;
		
		
	}
	
	public DistributedRowMatrix getMatrix(){
		return matrix;
	}
	public int getDesireRank(){
		return desireRank;
	}
	public int getIterationNumber(){
		return iterationNumber;
	}
	public void setIterationNumber(int i){
		iterationNumber = i;
	}
	//将向量q保存
	public void setIterVector(int i,ArrayDoubleWritable m) throws IOException{
		iterVector.put(i, m);
	}
	public ArrayDoubleWritable getIterVector(int i){
		return iterVector.get(i);
	}
	public void setEigenVector(int i,ArrayDoubleWritable m){
		eigenVector.put(i, m);
	}
	public ArrayDoubleWritable getEigenVector(int i){
		return eigenVector.get(i);
	}
	public double[][] getTriDiagonal(){
		return triDiagonal;
	}
	public void setTriDiagonal(int i,int j,double value){
		triDiagonal[i][j] = value;
	}

	
	
	public void printIterVector(){
		for(Integer tmp:iterVector.keySet()){
			System.out.println(tmp+"");
			System.out.println(iterVector.get(tmp).print());
		}
	}
	public void printEigenVector(){
		for(Integer tmp:eigenVector.keySet()){
			System.out.println(tmp+"");
			System.out.println(eigenVector.get(tmp).print());
		}
	}
	
	public void printTriDiagonal(){
		for(int i=0;i<desireRank;i++){
			for(int j=0;j<desireRank;j++){
				System.out.print(triDiagonal[i][j]+" ");
			}
			System.out.println();
		}
	}
}

