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

public class ReducerThree extends Reducer<Text, Text, Text, Text>{

	private byte[] buffer;
	private FileSystem hdfs;
	private FSDataInputStream in;
	
	
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
//		super.cleanup(context);
		in.close(); 
//	    hdfs.close(); 
	}

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
//		super.reduce(arg0, arg1, arg2);
		String[] avgs = new String(buffer).split("\t|\n");
		String[] item = arg0.toString().split("\t");
		float r1 = 0.0f, r2=0.0f;
		int size = avgs.length;
		int i;
		int flag = 0;
		for(i = 0;i < size - 1;i += 2){
			if(flag == 2){
				break;
			}
			if(avgs[i].equals(item[0])){
				r1 = Float.valueOf(avgs[i+1]);
				flag ++;
			}else if(avgs[i].equals(item[1])){
				r2 = Float.valueOf(avgs[i+1]);
				flag ++;
			}
		}
		double sim = 0.0;
		double r = 0.0;
		double rx = 0.0;
		double ry = 0.0;
		for(Text val : arg1){
			String[] values = val.toString().split("\t");
			r += (Float.valueOf(values[0]) - r1)*(Float.valueOf(values[1]) - r2);
			rx += (Float.valueOf(values[0]) - r1)*(Float.valueOf(values[0]) - r1);
			ry += (Float.valueOf(values[1]) - r2)*(Float.valueOf(values[1]) - r2);
		}
		double f = (Math.sqrt(rx)*Math.sqrt(ry));
		if(f == 0.0){
			sim = 0.0;
		}else {
			sim = r/f;
		}
		
		arg2.write(arg0, new Text(String.valueOf((float)sim)));
//		arg2.write(new Text(item[1] + "\t" + item[0]), new Text(String.valueOf((float)sim)));
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
//		super.setup(context);
		String src="hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/out/part-r-00000";
		hdfs = FileSystem.get(URI.create(src), context.getConfiguration());
		Path avg_file = new Path(src);
		in = hdfs.open(avg_file);
		FileStatus stat = hdfs.getFileStatus(avg_file); 
		buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))]; 
	    in.readFully(0, buffer); 
	    
	}
	
}
