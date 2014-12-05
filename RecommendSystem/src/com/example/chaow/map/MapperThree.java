package com.example.chaow.map;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperThree extends Mapper<Object, Text, Text, Text>{

	@Override
	protected void map(Object key, Text value,
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
//		super.map(key, value, context);
		String[] values = value.toString().split("\t");
		context.write(new Text(values[0] + "\t" + values[1]), new Text(values[2] + "\t" + values[3]));
	}
	
}
