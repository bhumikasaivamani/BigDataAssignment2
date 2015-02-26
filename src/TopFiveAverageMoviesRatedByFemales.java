
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class TopFiveAverageMoviesRatedByFemales 
{
    public static class MapRatings extends Mapper<LongWritable, Text, Text, Text> 
    {
        private Text word = new Text();
	        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split("::");
            word.set(tokens[0]); 
            String out = tokens[1];
            out+= "<>";
            out+= tokens[2];  //movieid<>rating
            String out1 = "R::";
            out1 += out;
            context.write(word, new Text(out1));

        }
    }
    
    public static class MapGender extends Mapper<LongWritable, Text, Text, Text> 
    {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
	
            String line = value.toString();
            String[] tokens = line.split("::");
            String userId = tokens[0];
            String gender = tokens[1];

            if(gender.equals("F"))
            {
                    String out = "G::";
                    //out += gender;
                    context.write(new Text(userId),new Text(out) );
            }
        }
    } 
    
    
    public static class ReduceToMovieIdAndRatings extends Reducer<Text, Text, Text, Text> 
    {
         private ArrayList<Text> User = new ArrayList<Text>();
         private ArrayList<Text> Rating = new ArrayList<Text>();
         private Text tmp;
         private Map<String,String> Ratings=new HashMap<>();
         private Map<String,String> Users=new HashMap<>();
        
        String total="";
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            Users.clear();
            Ratings.clear();
            Iterator<Text> value = values.iterator();

           
            while(value.hasNext())
            {
                tmp = value.next();
                String ip = tmp.toString();
           
                if(tmp.charAt(0) == 'R')
                {
                        String[] tk = ip.split("::");
                        //Rating.add(new Text(tk[1]));
                        Ratings.put(key.toString(),tk[1]);
                }

                else if(tmp.charAt(0) == 'G')
                {
                       // String[] lk = ip.split("::");
                        //User.add(new Text(lk[1]));
                        Users.put(key.toString(),"F");
                }
            }
            
            for(String s: Users.keySet())
            {
                if(Ratings.get(s)!=null)
                {
                    String[] tokens = Ratings.get(s).toString().split("<>");
                    context.write(new Text(tokens[0]), new Text(tokens[1]));
                }
            }
            
            /*int sum=0;
            String test="";

            if(!User.isEmpty() && !Rating.isEmpty())
            {

                for(Text A : User)
                {
                        for(Text B : Rating)
                        {
                            String[] tokens = B.toString().split("<>");
                            //test.put(new Text(tokens[0]), new Text(tokens[1]));
                            //context.write(new Text(tokens[0]), new Text(tokens[1]));
                            test=tokens[0];
                            sum=sum+Integer.parseInt(tokens[1]);
                            
                        }
                        total=Integer.toString(sum);
                        context.write(new Text(test),new Text(total));
                }*/

            
            
           //context.write(key,new Text(values));
           //context.write(new Text("******"),new Text("********"));*/

        }
    }
    
    public static class MapAverage extends Mapper<LongWritable, Text, Text, Text> 
    {
        //private final static IntWritable one = new IntWritable(1);
        //Mapper takes value from data set line by line and produces key value pair for each data row in the data set
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
	    String line=value.toString();
            String [] tokenizedLine=line.split("\t");
            String movieId=tokenizedLine[0];
            String rating=tokenizedLine[1];
            //Take Class of Age and Gender and make it as a key
            //String keyAgeClass=ageValue.concat(" ").toString().concat(gender);
            context.write(new Text(movieId),new Text(rating));
	 } 
    }
    
    public static class ReduceAverage extends Reducer<Text, Text, Text, Text> 
    {
        //private IntWritable total = new IntWritable();
        public double total;
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
	    int sum=0;
            int count=0;
            //TreeMap<Double,Text> map=new TreeMap<>(); 
            for (Text val : values) 
            {
                String v=val.toString().trim();
                //context.write(new Text(""),new Text(v));
                int i=Integer.parseInt(v);
                sum=sum+i;
                count++;
            }
            total=(double)((double)sum/(double)count);
            context.write(new Text(key), new Text(Double.toString(total))); //write to ouput file
        }
    }
    
    public static class MapAverageTop5 extends Mapper<LongWritable, Text, Text, Text> 
    {
        //private final static IntWritable one = new IntWritable(1);
        //Mapper takes value from data set line by line and produces key value pair for each data row in the data set
        private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
	    String line=value.toString();
            String [] tokenizedLine=line.split("\t");
            String movieId=tokenizedLine[0];
            String avgRating=tokenizedLine[1];
            //Take Class of Age and Gender and make it as a key
            //String keyAgeClass=ageValue.concat(" ").toString().concat(gender);
            //context.write(new Text(movieId),new Text(avgRating));
            repToRecordMap.put(Double.parseDouble(avgRating),new Text(movieId));
            if (repToRecordMap.size() > 5) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
	 } 
        
        @Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Double d:repToRecordMap.keySet()) {
				context.write(new Text(repToRecordMap.get(d)),new Text(Double.toString(d)));
			}
		}
    }
    
    public static class ReduceAverageTop5 extends Reducer<Text, Text, Text, Text> 
    {
        //private IntWritable total = new IntWritable();
        public double total;
	public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException 
        {
	   context.write(key, value);
        }
    }
    
    public static void main(String[] args) throws Exception 
    {
        JobConf conf1 = new JobConf();
        Job job1 = new Job(conf1, "TopFiveAverageMoviesRatedByFemales");
        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, TopFiveAverageMoviesRatedByFemales.MapRatings.class);
        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, TopFiveAverageMoviesRatedByFemales.MapGender.class);
    
        
        job1.setReducerClass(TopFiveAverageMoviesRatedByFemales.ReduceToMovieIdAndRatings.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setJarByClass(TopFiveAverageMoviesRatedByFemales.class);

        job1.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        
        boolean flag = job1.waitForCompletion(true);
        boolean flag1=false;
        boolean flag2=false;
        
        if(flag)
        {
            JobConf conf2 = new JobConf();
            Job job2 = new Job(conf2, "AverageCalculation");

            //org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, Map2_1.class);
            //org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, Map2_2.class);
            
            job2.setMapperClass(MapAverage.class);
            job2.setReducerClass(ReduceAverage.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setJarByClass(TopFiveAverageMoviesRatedByFemales.class);

            job2.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            flag1=job2.waitForCompletion(true);
        }
        
        if(flag1)
        {
            JobConf conf3 = new JobConf();
            Job job3 = new Job(conf3, "AverageCalculation");

            //org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, Map2_1.class);
            //org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, Map2_2.class);
            
            job3.setMapperClass(MapAverageTop5.class);
            job3.setReducerClass(ReduceAverageTop5.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            job3.setJarByClass(TopFiveAverageMoviesRatedByFemales.class);

            job3.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job3, new Path(args[3]));
            FileOutputFormat.setOutputPath(job3, new Path(args[4]));

            flag2=job3.waitForCompletion(true);
            
        }
    }
    
}
