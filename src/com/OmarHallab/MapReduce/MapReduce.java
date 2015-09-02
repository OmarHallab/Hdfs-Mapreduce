/**
 * 
 */
package com.OmarHallab.MapReduce;


/**
 * @author __Ghost_
 *
 */


//Importing the apache source files (based in the jar files)

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduce {
	
	// generic datatypes of Mapper            input key, input value, output key, output value
	
	public static class MyMap extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		String departmentName;
		
		public void map(LongWritable k, Text v, Context con)
				throws IOException, InterruptedException{
			
			String[] words = v.toString().split(",");
			int departmentNb = Integer.parseInt(words[3]);
			int sal = Integer.parseInt(words[2]);
			
			if(departmentNb == 11){
				
				departmentName = "Marketing";
				
			}else if (departmentNb == 12){
				
				departmentName = "Hr";
				
			}else if (departmentNb == 13){
				
				departmentName = "Finanace";
			
			}else{
				
				departmentName = "Other";
			
			}
				
			 con.write(new Text(departmentName), new IntWritable(sal));
			
		}
		
	}

	//Output value from Mapper will be an input value in the Reducer
	
	public static class MyRed extends Reducer <Text,IntWritable,Text,IntWritable>{
		
		public void reduce(Text departmentName,Iterable<IntWritable>slist,Context con)
				throws IOException, InterruptedException{
			
			int total = 0;
			for(IntWritable sal: slist){
				
				total+=sal.get();
			}
			
			con.write(departmentName, new IntWritable(total));
			
		}
	}
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		Configuration c = new Configuration();
		
		Path p1 = new Path(args[0]);
		Path p2 = new Path(args[1]);
		
	   Job j = new Job(c,"Myjob1");
	   j.setJarByClass(MapReduce.class);
	   j.setMapperClass(MyMap.class);
	   j.setReducerClass(MyRed.class);
	   j.setOutputKeyClass(Text.class);
	   j.setOutputValueClass(IntWritable.class);
	   
	   FileInputFormat.addInputPath(j, p1);
	   FileOutputFormat.setOutputPath(j, p2);
	   
	   System.exit(j.waitForCompletion(true) ? 0:1);
	   
	   
		
		

	}

}
