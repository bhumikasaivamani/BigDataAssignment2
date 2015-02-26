
import org.apache.hadoop.mapred.JobConf;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
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
public class TopFiveAverageNoviesRatedByFemales 
{
    //static int c=0;
    //static TreeMap<Text,Text> tm = new TreeMap<>();
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> 
    {
        private Text word = new Text();
	        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] tokens = line.split("::");
            word.set(tokens[0]);
            String out = tokens[1];
            out+= "<>";
            out+= tokens[2];  //movieid<>rating
            String out1 = "A::";
            out1 += out;
            context.write(word, new Text(out1));

        }
    }
    
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> 
    {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
	
            String line = value.toString();
            String[] tokens = line.split("::");
            String userId = tokens[0];
            String gender = tokens[1];

            if(gender.equals("F"))
            {
                    String out = "B::";
                    out += gender;
                    context.write(new Text(userId),new Text(out) );
            }


        }
    } 
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> 
    {
        private ArrayList<Text> User = new ArrayList<Text>();
        private ArrayList<Text> Rating = new ArrayList<Text>();
        
        //java.util.Map<Text,Text> test=new HashMap<>();
        //java.util.Map<Text,Text> Rating=new HashMap<>();
        
        private Text tmp;
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            User.clear();
            Rating.clear();
            Iterator<Text> value = values.iterator();

            while(value.hasNext())
            {

                tmp = value.next();
                String ip = tmp.toString();
           
                if(tmp.charAt(0) == 'A')
                {
                        String[] tk = ip.split("::");
                        Rating.add(new Text(tk[1]));
                }

                else if(tmp.charAt(0) == 'B')
                {
                        String[] lk = ip.split("::");
                        User.add(new Text(lk[1]));
                }

            }

            if(!User.isEmpty() && !Rating.isEmpty())
            {

                for(Text A : User)
                {
                        for(Text B : Rating)
                        {
                            String[] tokens = B.toString().split("<>");
                            //test.put(new Text(tokens[0]), new Text(tokens[1]));
                            context.write(new Text(tokens[0]), new Text(tokens[1]));
                        }
                }

            }
            
           

        }
    }
    
    public static class Map2_1 extends Mapper<LongWritable, Text, Text, Text> 
    {
		
	private Text word = new Text();
        
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
	
	    	String line = value.toString();
	        String[] tokens = line.split("\t");
	        word.set(tokens[0]);
	        String out = tokens[1];
	        
	          
	        String out1 = "C::";
	        out1 += out;
	        context.write(word, new Text(out1));
	        
	}
    }
    public static class Map2_2 extends Mapper<LongWritable, Text, Text, Text> 
    {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {

            String line = value.toString();
            String[] tokens = line.split("::");
            String MID = tokens[0];
            String Mname = tokens[1];
            String out = "D::";
            out+= Mname;
            context.write(new Text(MID), new Text(out));

        }
    }
    
    public static class Reduce1 extends Reducer<Text, Text, Text, Text> 
    {
        private ArrayList<Text> movies = new ArrayList<Text>();
        private ArrayList<Text> rating = new ArrayList<Text>();
        
        private Text tmp;
        
	    public void reduce(Text key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {
	        
	    	double sum = 0;
            double count = 0,avg = 0;
	    	movies.clear();
	    	rating.clear();
                
         
	    	
	    	Iterator<Text> value = values.iterator();
	    	String[] tokens = null;
	        while(value.hasNext()){
	        	
	        	tmp = value.next();
	        	String ip = tmp.toString();
	        	if(tmp.charAt(0) == 'C')
	        	{
	        		String[] tk = ip.split("::");
	        		rating.add(new Text(tk[1]));
	        		
	        	}
	        	
	        	else if(tmp.charAt(0) == 'D')
	        	{
	        		String[] lk = ip.split("::");
	        		movies.add(new Text(lk[1]));
	        	}
	        	
	        }
	         
	        if(!movies.isEmpty())
	        {
	        	for(Text t : rating)
	        	{
	        		sum += Integer.parseInt(t.toString());
	        		count++;
	        	}
	        	
	        	avg = (double)sum / count;
	        	
	        	Text out = movies.get(0);
	 	        	//String out1 = out.toString();
	 	        	//out1+= "\t";
	 	        	//out1+= Double.toString(avg);
	 	       context.write(new Text(out), new Text(Double.toString(avg)));
                        //test.put(new Text(out), new Text(Double.toString(avg)));
                      //context.write(new Text(s),new Text(rt.get(s)));
	 		
	        }
	        /*for(Text t:test.keySet())
                {
                   context.write(new Text(t),new Text(test.get(t))); 
                }*/
	       
                
	        
	        
	    }
	 }
    
    public static class Map2_3 extends Mapper<LongWritable, Text, Text, Text> 
    {
        TreeMap<Text,Text> test=new TreeMap<>();
        
	public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException 
        {
            int count=0;
            String line = value.toString();
            String[] tokens = line.split("\t");
            
            test.put(new Text(tokens[0]), new Text(tokens[1]));
            
            
            if (test.size() > 10) {
				test.remove(test.firstKey());
			}
            /*for(Text t: test.keySet())
            {
                
            }*/
            
            //context.write(new Text(tokens[0]), new Text(tokens[1]));

        }
        @Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : test.values()) {
				context.write(new Text(t),new Text(test.get(t)));
			}
		}
    }
    
    
    public static class Reduce2 extends Reducer<Text, Text, Text,Text> 
    {
        
        public void reduce(Text key,  Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
                int c=0;
                TreeMap<Text,Text> tm = new TreeMap<>();
                
                for(Text val : values)
                {
                    tm.put(new Text(key), val);
                }
                for(Text t:tm.keySet())
                {
                    if(c>5)
                        break;
                    
                    context.write(t,new Text(tm.get(t)));
                }
        }
    }
    public static <K, V extends Comparable<? super V>> java.util.Map<K,V> sortByValue( java.util.Map<K,V>  map )
    {
     //Map<K,V> result = new LinkedHashMap<>();
     java.util.Map<K,V> result=new LinkedHashMap<>();
     Stream <Entry<K,V>> st = map.entrySet().stream();

     st.sorted(Comparator.comparing(e -> e.getValue()))
          .forEach(e ->result.put(e.getKey(),e.getValue()));

     return result;
    }
    
    public static void main(String[] args) throws Exception 
    {
        JobConf conf1 = new JobConf();
        Job job1 = new Job(conf1, "TopFiveAverageNoviesRatedByFemales");
        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Map.class);
        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Map1.class);
    
        
        job1.setReducerClass(Reduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setJarByClass(TopFiveAverageNoviesRatedByFemales.class);

        job1.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        
        boolean flag = job1.waitForCompletion(true); 
        if(flag)
        {
        	JobConf conf2 = new JobConf();
            Job job2 = new Job(conf2, "HW_2_2_2");

            org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, Map2_1.class);
            org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, Map2_2.class);
            

            job2.setReducerClass(Reduce1.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setJarByClass(TopFiveAverageNoviesRatedByFemales.class);

            job2.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[4]));

            boolean flag1=job2.waitForCompletion(true);
        }
        
        if(flag)
        {
        	JobConf conf3 = new JobConf();
            Job job3 = new Job(conf3, "HW_2_2_2_3");

            FileInputFormat.addInputPath(job3, new Path(args[4]));

            job3.setMapperClass(Map2_3.class);
            job3.setReducerClass(Reduce2.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            job3.setJarByClass(TopFiveAverageNoviesRatedByFemales.class);

            job3.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job3, new Path(args[5]));

            job3.waitForCompletion(true);
        }
            
   }
    
}
