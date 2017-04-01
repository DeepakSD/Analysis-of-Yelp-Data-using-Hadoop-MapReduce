package yelp;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageRating {
	public static class Map extends Mapper<LongWritable, Text, Text,DoubleWritable>{	
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException 
		{
			String[] line = value.toString().split("::");
			context.write(new Text(line[2]),new DoubleWritable(Double.parseDouble(line[3])));	
		}	
	}

	public static class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
		HashMap<String,Double> businessMap=new HashMap<String,Double>();
		
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context) 
				throws IOException, InterruptedException {
			double tot=0;
			int count=0;
			for (DoubleWritable val:values){
				tot+=val.get();
				count++;
			}
			double avg=tot/count;
			businessMap.put(key.toString(),avg);
		}
		public void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			int i = 0;
			TreeMap<String, Double> final_map = new TreeMap<String, Double>(new sortByValue(businessMap));
			final_map.putAll(businessMap);
			for (Entry<String, Double> entry : final_map.entrySet()) {
				if(i<10)
				{
				context.write(new Text(entry.getKey()),new DoubleWritable(entry.getValue()));
				i++;
				}
				else
					break;
			}
			
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
			// create a job with name "Average Rating"
			Job job = new Job(conf, "Average Rating");
			job.setJarByClass(AverageRating.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			// set output key type
			job.setOutputKeyClass(Text.class);
			// set output value type
			job.setOutputValueClass(DoubleWritable.class);
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			//Wait till job completion
			System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
