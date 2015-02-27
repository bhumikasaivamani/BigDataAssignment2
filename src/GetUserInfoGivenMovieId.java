
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author bhumikasaivamani
 */
public class GetUserInfoGivenMovieId 
{
    public static class Map extends Mapper<LongWritable, Text, Text, Text> 
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
	    String line=value.toString();
            String [] tokenizedLine=line.split("::");
            String userId=tokenizedLine[0];
            String movieId=tokenizedLine[1];
            String rating=tokenizedLine[2];
            
            Configuration conf = context.getConfiguration();
            String givenMovieId = conf.get("movieId");
            
            int checkRating=Integer.parseInt(rating);
            if(movieId.equals(givenMovieId))
            {
                if(checkRating>=4)
                {
                    context.write(new Text(userId),new Text(rating));
                }
            }
         
	 } 
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> 
    {
       
	public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException 
        {
	    context.write(key, values);
        }
    }
    
    
    
    public static class MapWithJoin extends Mapper<LongWritable, Text, Text, Text> 
    {
		
        private java.util.Map<String, String> userInfo = new HashMap<String, String>();
        File F;

        @Override
        public void setup(Context context) throws IOException, InterruptedException  
        {

            //Configuration conf = context.getConfiguration();
            //Path[] p = DistributedCache.getLocalCacheFiles(conf);
            Path [] files=DistributedCache.getLocalCacheFiles(context.getConfiguration());
            
            F = new File(files[0].getName().toString());
            /*//Path[] cacheFiles = context.getLocalCacheFiles();
            //F=new File(cacheFiles[0].toString());
            //FileInputStream fileStream = new FileInputStream(cacheFiles[0].toString());
             Path[] cacheFiles =DistributedCache.getLocalCacheFiles(context.getConfiguration());*/
            BufferedReader br = new BufferedReader(new FileReader(F));
            String line=br.readLine();

            while((line=br.readLine())!= null)
            {
                String[] token = line.split("::");

                userInfo.put(token[0],token[1]+"\t"+token[2]);
            }
         }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {

            String line = value.toString();
            String [] tokens = line.split("\t");
            String userId = tokens[0];
            String rating = tokens[1];

            if(userInfo.get(userId)!=null)
            {
                 context.write(new Text(userId),new Text(userInfo.get(userId)));
            }

        }
    }
    
    public static class ReduceFinal extends Reducer<Text, Text, Text, Text> 
    {
       
	public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException 
        {
	    context.write(key, values);
        }
    }
    
    
    public static void main(String[] args) throws Exception 
    {
	Configuration conf = new Configuration();    
        conf.set("movieId", args[2]);
	Job job = new Job(conf, "GetUserInfoGivenMovieId");
        
	    
	job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
        
	job.setJarByClass(GetUserInfoGivenMovieId.class);
	job.setMapperClass(Map.class);
	job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);
	        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[3]));
	        
	boolean flag1=job.waitForCompletion(true);
        
        
        if(flag1)
        {
            Configuration conf2 = new Configuration();
            //FileSystem fs = FileSystem.get(conf2);
            //Path Intermediate = new Path(args[1]);
            //DistributedCache.addCacheFile(Intermediate.toUri(), conf2);
            //DistributedCache.addCacheFile(new URI(args[1]),conf2);

            Job job2 = new Job(conf2,"UserInfo");
            /*Job job2 = new Job(new Configuration());
            Configuration conf2 = job.getConfiguration();
            job2.setJobName("Join with Cache");
            DistributedCache.addCacheFile(new URI(args[1]), conf2);*/
            job2.addCacheFile(new URI(args[1]));
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setJarByClass(GetUserInfoGivenMovieId.class);
            job2.setMapperClass(MapWithJoin.class);
             //job2.setCombinerClass(Reduce.class);
            job2.setReducerClass(ReduceFinal.class);

            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job2, new Path(args[3]));
            FileOutputFormat.setOutputPath(job2, new Path(args[4]));
            job2.waitForCompletion(true);
        }
    }
    
}
