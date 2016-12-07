package assignment2;

import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordFreq {
	public static class CountMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			line = line.replaceAll(",", "");
			line = line.replaceAll("\\.", "");
			line = line.replaceAll("-", " ");
			line = line.replaceAll("\"", "");
			line=line.toLowerCase();
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			tokenizer.nextToken();
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}

	public static class CountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		private static String[][] top10 = new String[10][2];
		
		@Override
		public void setup(Context context) throws IOException {
			for(int i=0;i<10;i++){
					top10[i][0]="0";
					top10[i][1]="0";
			}
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key = word
			//sum = number of times
			int sum = 0;

			for (@SuppressWarnings("unused") IntWritable val : values) {
				sum++;
			}
			if(Integer.parseInt(top10[9][0])>sum)
				return;
			for(int i=0;i<10;i++){
				int tempInt = Integer.parseInt(top10[i][0]);
				String tempStr= top10[i][1].toString();
				
				if(sum>tempInt){
					top10[i][0]=Integer.toString(sum);
					top10[i][1]=key.toString();
					
					for(int j=9; j>i+1; j--){
						top10[j][0]=top10[j-1][0];
						top10[j][1]=top10[j-1][1];
					}
					if(i<9){
						top10[i+1][0]=Integer.toString(tempInt);
						top10[i+1][1]=tempStr;
					}
					break;
				}
			}
			
		}
		
		@Override 
		public void cleanup(Context context) throws IOException, InterruptedException{
			String string="";
			for(int i =0;i<10;i++)
				string+=top10[i][1]+'\t'+top10[i][0]+'\n';
			context.write(new Text(string),new IntWritable());
		}
		
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {

		final String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordFreq.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		if (args.length > 2){
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}

		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setJarByClass(WordCount.class);
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(
				args[0]+"data/plot_summaries.txt"));
		FileSystem fs = FileSystem.get(conf);
		
		// handle (e.g. delete) existing output path
		Path outputDestination = new Path(args[0]+args[1]);
		if (fs.exists(outputDestination)) {
			fs.delete(outputDestination, true);
		}

		// set output path & start job1
		FileOutputFormat.setOutputPath(job, outputDestination);
		int jobCompletionStatus = job.waitForCompletion(true) ? 0 : 1;
		//job.submit();
	}
}