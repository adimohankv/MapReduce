import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TransPart {
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			String sports = parts[4];
			context.write(new Text(sports), value);
			
		}
	}
	public static class PartClass extends Partitioner<Text,Text>{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String sports = key.toString();
			if(sports.equalsIgnoreCase("Puzzles"))return 0;
			else if (sports.equalsIgnoreCase("Exercise & Fitness"))return 1;
			else if (sports.equalsIgnoreCase("Gymnastics"))return 2;
			else if (sports.equalsIgnoreCase("Team Sports"))return 3;
			else if (sports.equalsIgnoreCase("Outdoor Recreation"))return 4;
			else if (sports.equalsIgnoreCase("Outdoor Play Equipment"))return 5;
			else if (sports.equalsIgnoreCase("Winter Sports"))return 6;
			else if (sports.equalsIgnoreCase("Jumping"))return 7;
			else if (sports.equalsIgnoreCase("Indoor Games"))return 8;
			else if (sports.equalsIgnoreCase("Combat Sports"))return 9;
			else if (sports.equalsIgnoreCase("Water Sports"))return 10;
			else if (sports.equalsIgnoreCase("Air Sports"))return 11;
			else if (sports.equalsIgnoreCase("Games"))return 12;
			else if (sports.equalsIgnoreCase("Dancing"))return 13;
			else return 14;
		}
	}
	
	public static class ReduceClass extends Reducer<Text,Text,Text,Text>{
		public void reduce (Text key,Text value,Context context) throws IOException, InterruptedException{
			context.write(key, value);
		}
	} 
	public static void main(String args[]) throws Exception {
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf, "PartitionText");
		  job.setJarByClass(TransPart.class);
		  job.setMapperClass(MapClass.class);
		  //job.setReducerClass(ReduceClass.class);
		  job.setPartitionerClass(PartClass.class);
		  job.setNumReduceTasks(15);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
		  //job.setOutputValueClass(NullWritable.class);
		  //job.setInputFormatClass(TextInputFormat.class);
		  //job.setOutputFormatClass(TextOutputFormat.class);
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);

		 }
}
