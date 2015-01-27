package edu.gatech.cse6242;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1 {

  public static class IncomingNodeMapper
       extends Mapper<Object, Text, Text, IntWritable>{
	   
	   private Text node = new Text();
		   
		   
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      String[] row = value.toString().split("\t");
      String tgt = row[1].toString();
      int weight = 0;
      weight = Integer.parseInt(row[2]);
      if(weight > 0){
		 node.set(tgt);
		 context.write(node, new IntWritable(weight));
	 }          
    }
  }

  public static class IncomingNodeReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
	
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);
    job.setMapperClass(IncomingNodeMapper.class);
    job.setCombinerClass(IncomingNodeReducer.class);
    job.setReducerClass(IncomingNodeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
