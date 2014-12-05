package com.example.chaow.reduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ReducerFive extends Reducer<Text, Text, Text, Text>{

	public static final int ITEM_NUM = 1683;
	public static final int USER_NUM = 944;
	
	private float[][] sim = new float[ITEM_NUM][ITEM_NUM];
	
	private byte[] avgbuffer, simbuffer;
	private FileSystem hdfs1, hdfs2;
	private FSDataInputStream avgin, simin;
	
	
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
//		super.cleanup(context);
		avgin.close();
		simin.close();
	}



	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
//		super.setup(context);
		String srcavg="hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/out/part-r-00000";
		String srcsim="hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/three/part-r-00000";
		hdfs1 = FileSystem.get(URI.create(srcavg), context.getConfiguration());
		Path avg_file = new Path(srcavg);
		Path sim_file = new Path(srcsim);
		avgin = hdfs1.open(avg_file);
		FileStatus avgstat = hdfs1.getFileStatus(avg_file); 
		avgbuffer = new byte[Integer.parseInt(String.valueOf(avgstat.getLen()))]; 
	    avgin.readFully(0, avgbuffer); 
	    hdfs2 = FileSystem.get(URI.create(srcsim), context.getConfiguration());
	    simin = hdfs2.open(sim_file);
	    FileStatus simstat = hdfs2.getFileStatus(sim_file);
	    simbuffer = new byte[Integer.parseInt(String.valueOf(simstat.getLen()))];
	    simin.readFully(0, simbuffer);
	}



	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
//		super.reduce(arg0, arg1, arg2);
		String[] avgs_str = new String(avgbuffer).split("\t|\n");
		String[] sim_str = new String(simbuffer).split("\t|\n");
		int avg_size = avgs_str.length;
		int sim_size = sim_str.length;
		float[] avgs = new float[ITEM_NUM];
		
		int i, j;
		
		for(i = 0;i < ITEM_NUM;i++){
			avgs[i] = -1.0f;
		}
		
		for(i = 0;i < avg_size - 1;i+=2){
			avgs[Integer.valueOf(avgs_str[i])] = Float.valueOf(avgs_str[i+1]);
		}
		int x, y;
		float sim_rate = 0.0f;
		for(i = 0;i < sim_size - 2;i += 3){
			x = Integer.valueOf(sim_str[i]);
			y = Integer.valueOf(sim_str[i+1]);
			sim_rate = Float.valueOf(Float.valueOf(sim_str[i+2]));
			sim[x][y] = sim_rate;
			sim[y][x] = sim_rate;
		}
		
		float[] rate = new float[ITEM_NUM];
		for(i = 0;i < rate.length;i++){
			rate[i] = 0.0f;
		}
		
		Float avg_user = 0.0f;
		int avg_num = 0;
		
		for (Text val : arg1){
			String[] values = val.toString().split("\t");	
			rate[Integer.valueOf(values[0])] = Float.valueOf(values[1]);
			avg_user += Float.valueOf(values[1]);
			avg_num++;
		}
		float epsilon = 0.000001f;
		double p = 0.0;
		double sim_sum = 0.0;
		float avg_point = avg_user/avg_num;
		double rate_sub = 0.0;
		
		for(i = 1;i < rate.length;i++){
			if(rate[i] > epsilon || avgs[i] < 0){
				continue;
			}
			sim_sum = 0.0;
			rate_sub = 0.0;
			for(j = 1;j < rate.length;j++){
				if(rate[j] <= epsilon){
					continue;
				}
				sim_sum += sim[i][j];
				rate_sub += (rate[j] - avg_point)*sim[i][j];
			}
			if(sim_sum <= epsilon && sim_sum >= -epsilon){
				p = avgs[i];
			}else{
				p = rate_sub/sim_sum + avgs[i];
			}
			arg2.write(new Text(arg0 + "\t" + i), new Text(String.valueOf((float)p)));
		}
		
//		arg2.write(arg0, new Text(arg1.toString()));
	}
	
}
