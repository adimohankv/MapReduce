package POS;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class StateWise {
	public static class MappClass extends Mapper<LongWritable,Text,IntWritable,Text>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			int store_id = Integer.parseInt(parts[0]);
			context.write(new IntWritable(store_id), value);
		}		
	}
	
	public static class PartClass extends Partitioner<IntWritable,Text>{
		@Override
		public int getPartition(IntWritable key, Text value, int numReduceTasks) {
			String parts[] = value.toString().split(",");
			String state = parts[4];
			if(state.equalsIgnoreCase("KAR"))return 0;
			else return 1;
		}
		
	}
	public static class ReduceClass extends Reducer<IntWritable,Text,IntWritable,Text>{
		public void reduce(IntWritable key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			int sumquantity = 0;
			String output = null;
			for (Text val : value){
				String parts[] = val.toString().split(",");
				int quantity = Integer.parseInt(parts[3]);
				sumquantity += quantity;
				String state =parts[4];
				output = sumquantity+"\t"+state;
			}
			context.write(key,new Text(output));
		}
	}
	public static void main(String args[]) throws Exception {
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf, "PartitionText");
		  job.setJarByClass(StateWise.class);
		  job.setMapperClass(MappClass.class);
		  job.setReducerClass(ReduceClass.class);
		  job.setPartitionerClass(PartClass.class);
		  job.setNumReduceTasks(2);
		  job.setMapOutputKeyClass(IntWritable.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setOutputKeyClass(IntWritable.class);
		  job.setOutputValueClass(Text.class);
		  //job.setOutputValueClass(NullWritable.class);
		  //job.setInputFormatClass(TextInputFormat.class);
		  //job.setOutputFormatClass(TextOutputFormat.class);
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);

		 }
}