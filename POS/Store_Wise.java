package POS;

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



public class Store_Wise {
	public static class MappClass extends Mapper<LongWritable,Text,Text,IntWritable>{
		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] parts = value.toString().split(",");
			String state = parts[4];
			int quantity = Integer.parseInt(parts[3]);
			int price = Integer.parseInt(parts[2]);
			int sales = price*quantity;
			context.write(new Text(state),new IntWritable(sales));
		}		
	}
	
	public static class ReduceClass extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce (Text key,Iterable<IntWritable> value,Context context ) throws IOException, InterruptedException{
			int totalsales = 0;
			for (IntWritable val:value){
				totalsales += val.get();
			}
			context.write(key,new IntWritable(totalsales));
		}
	}
	public static void main(String args[]) throws Exception {
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf, "PartitionText");
		  job.setJarByClass(StateWise.class);
		  job.setMapperClass(MappClass.class);
		  job.setReducerClass(ReduceClass.class);
		  //job.setNumReduceTasks(2);
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
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
