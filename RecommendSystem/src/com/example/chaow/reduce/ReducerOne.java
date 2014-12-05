package com.example.chaow.reduce;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ReducerOne extends Reducer<Text, Text, Text, Text>{
	
	private MultipleOutputs<Text,Text> mos;

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
//		super.cleanup(context);
		mos.close();
	}



	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
//		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
	}



	public void reduce(Text arg0, Iterable<Text> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Text result;
		float sum = 0.0f;
		int num = 0;
		String values[];
		Text[] values2;
		Set<String> myset = new TreeSet();
		for (Text val : arg1){
			myset.add(val.toString());
			values = val.toString().split("\t");
			sum += Float.valueOf(values[1]);
			num ++;
//			mos.write("mrone", values[0], new Text(arg0 + "\t" + values[1]),"hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/mrone/");
		}
		sum = sum / num;
		result = new Text(String.valueOf(sum));
		arg2.write(arg0, result);
//		mos.write("avg", arg0, result, "hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/avg/");
//		String res1="";
//		for(String res : myset){
//			mos.write("mrone", arg0, new Text(res + "\t" + result), "hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/mrone/");
////			res1 += res + "\t"; 
//		}
//		mos.write("mrone", arg0, new Text(res1 + result), "hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/mrone/");
		//super.reduce(arg0, arg1, arg2);
	}
	
}
