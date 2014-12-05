package com.example.chaow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Recommend {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf1 = new Configuration();
//	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String[] otherArgs = {"hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/in", "hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/out"};
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: wordcount <in> <out>");
	      System.exit(2);
	    }
	    
	    /*
	    Job job1 = new Job(conf1, "Recommend System");
	    job1.setJarByClass(com.example.chaow.Recommend.class);
	    job1.setMapperClass(com.example.chaow.map.MapperOne.class);
//	    job11.setCombinerClass(IntSumReducer.class);
	    job1.setReducerClass(com.example.chaow.reduce.ReducerOne.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
//	    job1.setOutputFormatClass(MapROneMultipleOut.class);
//	    MultipleOutputs.addNamedOutput(job1,"avg",TextOutputFormat.class,Text.class,Text.class);  
//        MultipleOutputs.addNamedOutput(job1,"mrone",TextOutputFormat.class,Text.class,Text.class);  
	    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
	    job1.waitForCompletion(true);
	    
	    Configuration conf2 = new Configuration();
	    Job job2 = new Job(conf2, "job2");
	    job2.setJarByClass(com.example.chaow.Recommend.class);
	    job2.setMapperClass(com.example.chaow.map.MapperTwo.class);
	    job2.setReducerClass(com.example.chaow.reduce.ReducerTwo.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job2, new Path("hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/in"));
	    FileOutputFormat.setOutputPath(job2, new Path("hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/two"));
	    job2.waitForCompletion(true);
	    
	    
	    Configuration conf3 = new Configuration();
	    Job job3 = new Job(conf3, "job3");
	    job3.setJarByClass(com.example.chaow.Recommend.class);
	    job3.setMapperClass(com.example.chaow.map.MapperThree.class);
	    job3.setReducerClass(com.example.chaow.reduce.ReducerThree.class);
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job3, new Path("hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/two"));
	    FileOutputFormat.setOutputPath(job3, new Path("hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/three"));
	    job3.waitForCompletion(true);
	    
	    */
	    /**/
	    Configuration conf4 = new Configuration();
	    Job job4 = new Job(conf4, "job4");
	    job4.setJarByClass(com.example.chaow.Recommend.class);
	    job4.setMapperClass(com.example.chaow.map.MapperFour.class);
	    job4.setReducerClass(com.example.chaow.reduce.ReducerFive.class);
	    job4.setOutputKeyClass(Text.class);
	    job4.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job4, new Path("hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/in"));
	    FileOutputFormat.setOutputPath(job4, new Path("hdfs://127.0.0.1:8020/opt/hadoop-hbase/tmp/four"));
	    /**/
	    System.exit(job4.waitForCompletion(true) ? 0 : 1);
	}

}
