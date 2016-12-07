package assignment2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Index
{
  public static String string = "";
  
  public static class mapper1
    extends Mapper<Object, Text, Text, IntWritable>
  {
    public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException
    {
      String line = value.toString();
      line = line.replaceAll(",", "");
      line = line.replaceAll("\\.", "");
      line = line.replaceAll("-", " ");
      line = line.replaceAll("\"", "");
      line = line.toLowerCase();
      
      StringTokenizer tokenizer = new StringTokenizer(line);
      int movieID = Integer.parseInt(tokenizer.nextToken());
      while (tokenizer.hasMoreTokens())
      {
        String word = tokenizer.nextToken();
        context.write(new Text(word), new IntWritable(movieID));
      }
    }
  }
  
  public static class reducer1
    extends Reducer<Text, IntWritable, Text, Text>
  {
    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      Map<String, Integer> movieMap = new HashMap();
      for (IntWritable val : values) {
        if (!movieMap.containsKey(val.toString())) {
          movieMap.put(val.toString(), Integer.valueOf(1));
        } else {
          movieMap.put(val.toString(), Integer.valueOf(((Integer)movieMap.get(val.toString())).intValue() + 1));
        }
      }
      String output = movieMap.toString();
      output = output.replace("{", "");
      output = output.replace("}", "");
      output = output.replace("=", " ");
      context.write(new Text(key.toString()), new Text(output));
    }
  }
  
  public static void main(String[] args)
    throws Exception
  {
    String NAME_NODE = "hdfs://sandbox.hortonworks.com:8020";
    Configuration conf1 = new Configuration();
    Job job1 = Job.getInstance(conf1);
    job1.setJarByClass(Index.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setNumReduceTasks(1);
    job1.setReducerClass(reducer1.class);
    job1.setMapperClass(mapper1.class);
    FileInputFormat.addInputPath(job1, new Path(
      args[0] + "data/plot_summaries.txt"));
    FileSystem fs1 = FileSystem.get(conf1);
    Path outputDestination1 = new Path(args[0] + args[1]);
    if (fs1.exists(outputDestination1)) {
      fs1.delete(outputDestination1, true);
    }
    FileOutputFormat.setOutputPath(job1, outputDestination1);
    int jobCompletionStatus1 = job1.waitForCompletion(true) ? 0 : 1;
  }
}
