package yelp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
 
public class MapJoin extends Configured implements Tool
{
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		  
        private static HashSet<String> businessSet = new HashSet<String>();
        private BufferedReader brReader;
        private String businessId = "";
        private Text outKey = new Text("");
        private Text outValue = new Text("");
  
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
  
                Path[] cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
  
                for (Path filePath : cacheFile) {
                        if (filePath.getName().toString().trim().equals("business.csv")) {
                                setHashSet(filePath, context);
                        }
                }
	        }
                
        private void setHashSet(Path filePath, Context context) throws IOException {
  
                String line = "";
                brReader = new BufferedReader(new FileReader(filePath.toString()));
                while ((line = brReader.readLine()) != null) {
                	String businessData[] = line.toString().split("::");
                    if(businessData[1].contains("Stanford"))
                    {
                    	businessSet.add(businessData[0].trim());
                    }
               }      
        }
  
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                        String reviewData[] = value.toString().split("::");
                        
                        businessId = reviewData[2].toString().trim();
                        if (businessSet.contains(businessId))
                        	{
                        		outKey.set(reviewData[1].toString());
                            	outValue.set(reviewData[3].toString());
                            	context.write(outKey, outValue);
                            	businessId="";
                        	}
                businessId = "";
        }
}
    public static void main( String[] args ) throws Exception
    {
            int exitCode = ToolRunner.run(new Configuration(),new MapJoin(), args);
            System.exit(exitCode);
        }
    
	@Override
    public int run(String[] args) throws Exception {
            if(args.length !=3 ){
                    System.err.println("Insufficient Arguments");
                    System.exit(2);
            }
            Job job = new Job(getConf());
            Configuration conf = job.getConfiguration();
            job.setJobName("MapSideJoin using distributedCache");

            job.setJarByClass(MapJoin.class);
            DistributedCache.addCacheFile(new URI(args[0]),conf);
            FileInputFormat.addInputPath(job,new Path(args[1]) );
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            job.setMapperClass(Map.class);
            job.setNumReduceTasks(0);
             
            boolean success = job.waitForCompletion(true);
                return success ? 0 : 1;
             
    }
	
}