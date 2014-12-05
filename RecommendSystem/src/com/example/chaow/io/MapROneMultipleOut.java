package com.example.chaow.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MapROneMultipleOut extends MultipleOutputFormat{

	@Override
	protected String generateFileNameForKeyValue(WritableComparable key,
			Writable value, Configuration conf) {
		// TODO Auto-generated method stub
		String values[] = value.toString().split("\t");
		if(values.length == 1){
			return "/avg/avg_file.txt";
		}
		return "/res/mrone.txt";
//		return null;
	}

}
