import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class Wordcount {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,IntWritable>{
	
		Text word = new Text();
		IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>{
		IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val:values){
				sum = sum + val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String args[]) throws Exception {
		  Configuration conf = new Configuration();
		  conf.set("mapreduce.output.textoutputformat.separator", ",");
		  Job job = Job.getInstance(conf, "STD Calls");
		  job.setJarByClass(Wordcount.class);
		  job.setMapperClass(MapClass.class);
		  job.setReducerClass(ReduceClass.class);
		  job.setReducerClass(ReduceClass.class);
		  job.setOutputKeyClass(Text.class);
		  //job.setNumReduceTasks(0);
		  job.setOutputValueClass(IntWritable.class);
		  job.setInputFormatClass(TextInputFormat.class);
		  job.setOutputFormatClass(TextOutputFormat.class);
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);

		 }

}