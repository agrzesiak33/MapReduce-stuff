package assignment2;

import java.io.*;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Join {

	//Mapper for the Movie data
	public static class MovieMapper extends Mapper<Object,Text, IntWritable, Text>{
		
		@Override 
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException{
			//get the movie id and pass it in as the key
			//get the movie name and pass it in as the value
			
			String line = value.toString();
			line=line.replaceAll("\t\t\t\t","\tsomething\tsomething\tsomething\t");
			line=line.replaceAll("\t\t\t","\tsomething\tsomething\t");
			line=line.replaceAll("\t\t","\tsomething\t");
			
			StringTokenizer tokenizer = new StringTokenizer(line,"\t");
			int movieID = Integer.parseInt(tokenizer.nextToken());
			tokenizer.nextToken();
			String movieName="&";
			movieName+= tokenizer.nextToken();
			if(!movieName.contains("something"))
				context.write(new IntWritable(movieID), new Text(movieName));
		}
	}
	
	
	//Mapper for the character data
	public static class CharacterMapper extends Mapper<Object, Text, IntWritable, Text>{

		
		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException{
			//get the movie id and pass it in as the key
			//get the movie character name
			String line = value.toString();
			String temp3=line.replaceAll("\t\t\t\t","\tsomething\tsomething\tsomething\t");
			String temp2=temp3.replaceAll("\t\t\t","\tsomething\tsomething\t");
			String temp1=temp2.replaceAll("\t\t","\tsomething\t");
			
			
			
			StringTokenizer tokenizer = new StringTokenizer(temp1,"\t");
			int movieID = Integer.parseInt(tokenizer.nextToken());
			for(int i=0;i<7;i++)
				tokenizer.nextToken();
			
			String character = tokenizer.nextToken();
			if(!character.equals("something"))
				context.write(new IntWritable(movieID),new Text(character));
			
			
		}
	}
	
	public static class reducer extends 
			Reducer<IntWritable, Text, Text, Text>{
		
		@Override
		public void reduce(IntWritable key, Iterable<Text>values, Context context)
				throws IOException, InterruptedException{
			int movieID=Integer.parseInt(key.toString());
			String movieName="Not Available",characters="";
			for(Text val:values){
				String value=val.toString();
				if(value.contains("&")){
					movieName=value;
					movieName=movieName.replace("&", "");}
				else
					characters+=value+", ";
			}
			context.write(new Text(Integer.toString(movieID)+"\t"+movieName+", "+characters), new Text());
		}
	}
	
	@SuppressWarnings("unused")
	public static void main(String[] args)throws Exception{
		
		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		
		//Job to get the movie number and movie name
		Job movieJob = Job.getInstance(conf);
		movieJob.setJarByClass(Join.class);
		movieJob.setMapOutputKeyClass(IntWritable.class);//change
		movieJob.setMapOutputValueClass(Text.class);//change
		movieJob.setOutputKeyClass(IntWritable.class);
		movieJob.setOutputValueClass(Text.class);
		if(args.length>2)
			movieJob.setNumReduceTasks(Integer.parseInt(args[2]));
		movieJob.setNumReduceTasks(1);
		movieJob.setReducerClass(reducer.class);
		
		MultipleInputs.addInputPath(movieJob, new Path(args[0]+
				"data/movie.metadata.tsv"), TextInputFormat.class,MovieMapper.class);
		MultipleInputs.addInputPath(movieJob, new Path(args[0]+
				"data/character.metadata.tsv"), TextInputFormat.class,CharacterMapper.class);
		
		
		FileSystem fs = FileSystem.get(conf);
		
		Path outputDestination = new Path(args[0]+args[1]);
		if (fs.exists(outputDestination)) 
			fs.delete(outputDestination, true);
		
		FileOutputFormat.setOutputPath(movieJob, outputDestination);
		int jobCompletionStatus = movieJob.waitForCompletion(true) ? 0 : 1;		
		
	}
	
}