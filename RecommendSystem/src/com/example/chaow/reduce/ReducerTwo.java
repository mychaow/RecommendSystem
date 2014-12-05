package com.example.chaow.reduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ReducerTwo extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,
			Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Map<Integer, Float> map = new TreeMap<Integer, Float>();
		for (Text val : arg1){
			String[] values = val.toString().split("\t");
			map.put(Integer.valueOf(values[0]), Float.valueOf(values[1]));
		}
		Iterator it = map.entrySet().iterator();
		int i,j;
		i = 1;
		String k1="", v1="";
		Map.Entry k1entry, k2entry;
		while(it.hasNext()){
			k1entry = (Map.Entry) it.next();   
			Iterator itt = map.entrySet().iterator();
			j = 0;
			
			while(j < i){
				itt.next();
				j++;
			}
			i++;
			while(itt.hasNext()){
				k1 = k1entry.getKey() + "\t";
				v1 = k1entry.getValue() + "\t";
				k2entry = (Map.Entry) itt.next();  
				k1 += k2entry.getKey();
				v1 += k2entry.getValue();
//				mos.write("two", new Text(k1), new Text(v1), "hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/two");
				arg2.write(new Text(k1), new Text(v1));
			}
		}
	}

	
}
