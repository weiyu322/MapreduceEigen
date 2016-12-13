package com.myhadoop.matrixdecomposition.lanczos;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;

import com.myhadoop.matrixdecomposition.datamodel.DistributedRowMatrix;

public class LanczosMain {

	/**
	 * @param args
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException 
	{
		/*
		 * args:input directory,matrix size,desireRank
		 */
		System.out.println("start");
		long startall = System.currentTimeMillis();
		// TODO Auto-generated method stub
		String root = "lanczosoutput";
		Path in = new Path("lanczosinput");
		Path out = new Path(root,"matrix");
		Path tmp = new Path(root,"tmp");
		int size = 1000000;
		int desireRank = 80;
		//LanczosTimer.init();
		//LanczosTimer.start("AdjacentMatrixInputJob");
		long start_matread = System.currentTimeMillis();
		AdjacentMatrixInputJob.runJob(in, out);
		long end_matread = System.currentTimeMillis();
		//LanczosTimer.end("AdjacentMatrixInputJob");
		
		DistributedRowMatrix mat = new DistributedRowMatrix(out,tmp,size);
		
		Path EigenVector = new Path(root,"eigenvector");
		LanczosStatus ls = new LanczosStatus(mat,desireRank);
		//ls.printTriDiagonal();
		long start_lanczos = System.currentTimeMillis();
		HadoopLanczosSolver solver = new HadoopLanczosSolver();
		double[] ev = solver.solve(ls, EigenVector);
		long end_lanczos = System.currentTimeMillis();
		
		for (int i=0;i<ev.length;i++)
		{
			System.out.println(ev[i]);
		}
		
		//ls.printIterVector();
		long endall = System.currentTimeMillis();
		System.out.println("finish");
		System.out.println("Totaltime: "+(endall-startall)/1000.0+" s");
		System.out.println("Matread time: "+(end_matread-start_matread)/1000.0+" s");
		System.out.println("Lanczos time: "+(end_lanczos-start_lanczos)/1000.0+" s");
	}

}
