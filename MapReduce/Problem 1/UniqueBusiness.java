package yelp;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UniqueBusiness {
	public static class Map extends Mapper<LongWritable, Text, Text,NullWritable>{
		private Set<String> uniqueSet = new HashSet<String>();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("::");
			if(line[1].contains("Palo Alto"))
			{
				int length=line[2].length();
				String business=line[2].substring(5,length-1);
				String[] categories=business.split(",");
				for (String item:categories)
				{
					if(!uniqueSet.contains(item.trim()))
					{
						uniqueSet.add(item.trim());
						context.write(new Text(item.trim()),NullWritable.get());
					}	
				}
			}
		}
	}
	
	
	public static class Reduce extends Reducer<Text,NullWritable,Text,NullWritable> {
		public void reduce(Text key, NullWritable values,Context context) throws IOException, InterruptedException {
			context.write(key,NullWritable.get());
		}
	}
	
	
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Insufficient Arguments");
			System.exit(2);
		}
		// create a job with name "UniqueBusiness"
		Job job = new Job(conf, "UniqueBusiness");
		job.setJarByClass(UniqueBusiness.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(NullWritable.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
