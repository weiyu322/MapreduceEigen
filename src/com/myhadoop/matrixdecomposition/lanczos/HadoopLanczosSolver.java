package com.myhadoop.matrixdecomposition.lanczos;

import java.io.IOException;
import java.net.URISyntaxException;

import com.myhadoop.matrixdecomposition.datamodel.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

public class HadoopLanczosSolver
{
	
	public double[] solve(LanczosStatus ls, Path out) throws IOException,
			URISyntaxException, InterruptedException, ClassNotFoundException
	{
		//i初始为1
		int i = ls.getIterationNumber();
		ArrayDoubleWritable currentVector = ls.getIterVector(i);	//q_1
		ArrayDoubleWritable previousVector = ls.getIterVector(i-1);	//q_0
		DistributedRowMatrix A = ls.getMatrix();	//矩阵A
		double beta = 0;							//beta_0
		int desireRank = ls.getDesireRank();		//lanczos迭代中的迭代次数m
		//LanczosTimer.start("AllIteration");
		
/*******************************lanczos迭代核心代码段******************************************/
		while (i <= desireRank)
		{
			System.out.println("MatrixMulIteration Number:"+i);
			//LanczosTimer.start("Iteration-"+i+"Mul");
			//mapreduce求最矩阵向量乘法
			ArrayDoubleWritable nextVector = MatrixMulArrayDoubleWritableJob.runJob(A,
					currentVector);					//u_k=A*q_k				
			//System.out.println("u_k: "+nextVector.print());

			//LanczosTimer.end("Iteration-"+i+"Mul");

			double alpha = currentVector.dotVector(nextVector);		//a_k=q_k.*u_k
			//System.out.println("alpha:"+alpha);
			if (previousVector != null)
			{
				nextVector.plusAlphaMulVector(previousVector, -beta);	//u_k-beta_k-1*q_k-1
			}
			//System.out.println(nextVector.print());
			
			nextVector.plusAlphaMulVector(currentVector, -alpha);	//w_k=u_k-beta_k-1*q_k-1-a_k*q_k
			//System.out.println("w_k: "+nextVector.print());
			
			//LanczosTimer.start("orthoganalize-"+i);
			/*
			for (int j = 0; j < ls.getIterationNumber(); j++)
			{
				ArrayDoubleWritable basisVector = ls.getIterVector(j);
				double al;
				if (basisVector == null
						|| (al = nextVector.dotVector(basisVector)) == 0.0)
				{
					continue;
				}
				nextVector.plusAlphaMulVector(basisVector, -al);
			}
			*/
			//System.out.println(nextVector.print());
			//LanczosTimer.end("orthoganalize-"+i);
			ls.setTriDiagonal(i - 1, i - 1, alpha);		//alpha加入三对角矩阵
			
			
			beta = nextVector.norm2();			//beta_k=||w_k||
			//System.out.println("beta_k: "+beta);
			//迭代提前终止条件：当前beta小于阈值
			/*
			if(beta < 1E-15){
				break;
			}
			*/
			nextVector.mulConst(1 / beta);			//q_k+1=w_k/beta_k

			//System.out.println("q_k+1: "+nextVector.print());
			//更新LanczosStatus对象信息
			if (i < desireRank)
			{
				ls.setIterVector(i+1, nextVector);
				if (i > 1)
				{	
					ls.setTriDiagonal(i - 2, i - 1, beta);
					ls.setTriDiagonal(i - 1, i - 2, beta);
				}
				previousVector = currentVector;
				currentVector = nextVector;
				ls.setIterationNumber(++i);
			}
			else{
				break;
			}

		}
		//LanczosTimer.end("AllIteration");
//		ls.printTriDiagonal();
		//LanczosTimer.start("CalcEigenTriDiagonal");
/*******************************调用QR单机算法求三对角矩阵特征值特征向量***********************************/
		System.out.println("Start QR");
		//ls.printTriDiagonal();
		//double[][] tri = ls.getTriDiagonal();
		//System.out.println(tri.length);
		EigenvalueDecomposition decomp = new EigenvalueDecomposition(
				ls.getTriDiagonal(), ls.getIterationNumber());
		double[] ev = decomp.getEV();
		//LanczosTimer.end("CalcEigenTriDiagonal");
//		decomp.printVector();
		desireRank = ls.getIterationNumber();
		//LanczosTimer.start("CalcLanczosEigenVector");
		for (int row = 0; row < desireRank; row++)
		{
			ArrayDoubleWritable realEigen = new ArrayDoubleWritable(A.getDim());
			double[] ejCol = decomp.getCol(desireRank-row-1);
			for (int j = 0; j < desireRank; j++)
			{
				double d = ejCol[j];
				ArrayDoubleWritable rowJ = ls.getIterVector(j);
				realEigen.plusAlphaMulVector(rowJ, d);
			}
//			realEigen.normalize();
			ls.setEigenVector(row, realEigen);
		}
		//LanczosTimer.end("CalcLanczosEigenVector");
//		ls.printEigenVector();
		
		//写入hdfs，位置为out
		//LanczosTimer.start("WriteLanczosEigenVector");
		serializeOutput(ls, out, desireRank);
		//LanczosTimer.end("WriteLanczosEigenVector");
		System.out.println("End QR");

		return ev;
		
	}
	
	//将特征向量写入HDFS
	public void serializeOutput(LanczosStatus ls,Path out,int clusters) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(out.toUri(), conf);
		fs.delete(out, true);
	    SequenceFile.Writer seqWriter =
	        new SequenceFile.Writer(fs, conf, out, IntWritable.class, ArrayDoubleWritable.class);
	    for(int i=0; i<clusters;i++){
	    	IntWritable col = new IntWritable(i);
	    	ArrayDoubleWritable val = ls.getEigenVector(i);
	    	seqWriter.append(col, val);
	    }
	    seqWriter.close();
	}
}
