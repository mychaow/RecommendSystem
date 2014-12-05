package com.example.chaow.map;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperFour extends Mapper<Object, Text, Text, Text>{

	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
//		super.map(key, value, context);
		Text key1, val1;
		String values[] = value.toString().split("\t");
		key1 = new Text(values[0]);
		val1 = new Text(values[1] + "\t" + values[2]);
		context.write(key1, val1);
		
	}
	
}
